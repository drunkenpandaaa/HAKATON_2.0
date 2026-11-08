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

func TestCancelOrderSharedProducts(t *testing.T) {
	pool := testPool(t)
	s := New(pool)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Заказ А владеет тестовыми товарами:
	// остатки 5 и 10, возвращаемые количества 2 и 1.
	a := createFixture(t, ctx, pool, []testItem{
		{stock: 5, quantity: 2},
		{stock: 10, quantity: 1},
	})

	// Заказ Б использует те же товары.
	// Он создан позже, поэтому его cleanup выполнится раньше cleanup А.
	b := createFixture(t, ctx, pool, nil)

	// Позиции Б записываем в обратном порядке ID товаров.
	for _, item := range []struct {
		productID int64
		quantity  int
	}{
		{productID: a.productIDs[1], quantity: 4},
		{productID: a.productIDs[0], quantity: 3},
	} {
		_, err := pool.Exec(ctx, `
			INSERT INTO order_items
				(order_id, product_id, product_name, quantity, unit_price_cents)
			VALUES ($1, $2, 'Test product', $3, 100)
		`, b.orderID, item.productID, item.quantity)
		if err != nil {
			t.Fatal(err)
		}
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup

	for _, id := range []int64{a.orderID, b.orderID} {
		wg.Add(1)

		go func(orderID int64) {
			defer wg.Done()
			<-start
			results <- s.CancelOrder(ctx, orderID)
		}(id)
	}

	close(start)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Errorf("cancel shared-products order: %v", err)
		}
	}

	// 5 + 2 + 3 = 10.
	// 10 + 1 + 4 = 15.
	checkState(t, ctx, pool, a, "cancelled", []int{10, 15})
	checkState(t, ctx, pool, b, "cancelled", nil)

	// Повторная отмена обоих заказов ничего не возвращает.
	for _, id := range []int64{a.orderID, b.orderID} {
		if err := s.CancelOrder(ctx, id); err != nil {
			t.Fatalf("repeat cancellation of order %d: %v", id, err)
		}
	}

	checkState(t, ctx, pool, a, "cancelled", []int{10, 15})
	checkState(t, ctx, pool, b, "cancelled", nil)
}
