package store

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Database != "inventory_test" {
		t.Fatal("tests must use the inventory_test database")
	}

	// Несколько соединений нужны для конкурентных вызовов.
	cfg.MaxConns = 12

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}

	return pool
}

type testItem struct {
	stock    int
	quantity int
}

type fixture struct {
	orderID    int64
	productIDs []int64
}

func createFixture(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	items []testItem,
) *fixture {
	t.Helper()

	f := &fixture{}

	// Cleanup выполняется и при падении теста.
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()

		_, err := pool.Exec(cleanupCtx,
			`DELETE FROM order_items WHERE order_id = $1`, f.orderID)
		if err != nil {
			t.Errorf("cleanup items: %v", err)
		}

		_, err = pool.Exec(cleanupCtx,
			`DELETE FROM orders WHERE id = $1`, f.orderID)
		if err != nil {
			t.Errorf("cleanup order: %v", err)
		}

		for _, id := range f.productIDs {
			_, err := pool.Exec(cleanupCtx,
				`DELETE FROM products WHERE id = $1`, id)
			if err != nil {
				t.Errorf("cleanup product: %v", err)
			}
		}
	})

	err := pool.QueryRow(ctx, `
		INSERT INTO orders (customer_email, status, total_cents)
		VALUES ('test@example.com', 'created', 0)
		RETURNING id
	`).Scan(&f.orderID)
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range items {
		var productID int64
		err := pool.QueryRow(ctx, `
			INSERT INTO products (sku, name, price_cents, stock)
			VALUES (
				'test-' || $1::bigint::text || '-' || $2::integer::text,
				'Test product', 100, $3
			)
			RETURNING id
		`, f.orderID, len(f.productIDs), item.stock).Scan(&productID)
		if err != nil {
			t.Fatal(err)
		}
		f.productIDs = append(f.productIDs, productID)

		_, err = pool.Exec(ctx, `
			INSERT INTO order_items
				(order_id, product_id, product_name, quantity, unit_price_cents)
			VALUES ($1, $2, 'Test product', $3, 100)
		`, f.orderID, productID, item.quantity)
		if err != nil {
			t.Fatal(err)
		}
	}

	return f
}

func checkState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	f *fixture,
	wantStatus string,
	wantStocks []int,
) {
	t.Helper()

	var status string
	err := pool.QueryRow(ctx,
		`SELECT status FROM orders WHERE id = $1`,
		f.orderID,
	).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}
	if status != wantStatus {
		t.Errorf("status = %q, want %q", status, wantStatus)
	}

	for i, id := range f.productIDs {
		var stock int
		err := pool.QueryRow(ctx,
			`SELECT stock FROM products WHERE id = $1`, id,
		).Scan(&stock)
		if err != nil {
			t.Fatal(err)
		}
		if stock != wantStocks[i] {
			t.Errorf("product %d stock = %d, want %d",
				id, stock, wantStocks[i])
		}
	}
}

func TestCancelOrder(t *testing.T) {
	pool := testPool(t)
	s := New(pool)

	t.Run("normal_and_repeated", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f := createFixture(t, ctx, pool, []testItem{
			{stock: 8, quantity: 2},
			{stock: 19, quantity: 1},
		})

		for attempt := 0; attempt < 2; attempt++ {
			if err := s.CancelOrder(ctx, f.orderID); err != nil {
				t.Fatalf("attempt %d: %v", attempt+1, err)
			}
			checkState(t, ctx, pool, f, "cancelled", []int{10, 20})
		}
	})

	t.Run("not_found", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f := createFixture(t, ctx, pool, nil)

		// Удаляем свой заказ, чтобы гарантированно получить отсутствующий ID.
		_, err := pool.Exec(ctx,
			`DELETE FROM orders WHERE id = $1`, f.orderID)
		if err != nil {
			t.Fatal(err)
		}

		if err := s.CancelOrder(ctx, f.orderID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("concurrent_same_order", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f := createFixture(t, ctx, pool, []testItem{
			{stock: 8, quantity: 2},
			{stock: 19, quantity: 1},
		})

		const workers = 10
		start := make(chan struct{})
		results := make(chan error, workers)
		var wg sync.WaitGroup

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				results <- s.CancelOrder(ctx, f.orderID)
			}()
		}

		close(start)
		wg.Wait()
		close(results)

		for err := range results {
			if err != nil {
				t.Errorf("concurrent cancellation: %v", err)
			}
		}

		checkState(t, ctx, pool, f, "cancelled", []int{10, 20})
	})

	t.Run("rollback_on_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		f := createFixture(t, ctx, pool, []testItem{
			{stock: 8, quantity: 2},
			{stock: 2147483647, quantity: 1},
		})

		// Второе обновление вызовет переполнение PostgreSQL INTEGER.
		err := s.CancelOrder(ctx, f.orderID)

		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			t.Fatalf("expected PostgreSQL error, got %v", err)
		}
		if pgErr.Code != "22003" {
			t.Fatalf("expected numeric overflow (22003), got %s: %v",
				pgErr.Code, err)
		}

		// Первое обновление тоже должно откатиться.
		checkState(t, ctx, pool, f, "created", []int{8, 2147483647})
	})
}
