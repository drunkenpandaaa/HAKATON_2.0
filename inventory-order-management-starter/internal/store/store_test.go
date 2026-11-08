package store

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hackathon/inventory-order-management/internal/domain"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCreateOrder(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/inventory_test?sslmode=disable")
	if err != nil {
		t.Fatalf("Err to connect test database:%v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Err to ping test database:%v", err)
	}
	store := New(pool)
	randSku := "test" + strconv.Itoa(rand.Int())
	product, err := store.CreateProduct(ctx, randSku, "rtx-3060", 2500000, 5)
	if err != nil {
		t.Fatalf("Failed to create product:%v", err)
	}
	var domainInput domain.CreateOrderInput
	var OrderItemInput domain.OrderItemInput
	// var OrderItem domain.OrderItem
	// OrderItem.ProductID = product.ID
	// OrderItem.ProductName = product.Name
	// OrderItem.Quantity = product.Stock-3
	// OrderItem.UnitPrice = product.Price-10000
	OrderItemInput.ProductID = product.ID
	OrderItemInput.Quantity = 3
	domainInput.CustomerEmail = "test@mail.ru"
	domainInput.Items = []domain.OrderItemInput{}
	domainInput.Items = append(domainInput.Items, OrderItemInput)
	order, err := store.CreateOrder(ctx, domainInput)
	if err != nil {
		t.Fatalf("Failed to create order: %v", err)
	}
	if order.Total != 7500000 {
		t.Errorf("Non right total, waited 7500000")
	}
	var stock int64
	err = pool.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1`, product.ID).Scan(&stock)
	if err != nil {
		t.Fatalf("Error to select from db:%v", err)
	}
	if stock != 2 {
		t.Fatalf("Not right stock in db")
	}
}
func TestConcurrentCreateOrder(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/inventory_test?sslmode=disable")
	if err != nil {
		t.Fatalf("Err to connect test database:%v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Err to ping test database:%v", err)
	}
	store := New(pool)
	randSku := "test" + strconv.Itoa(rand.Int())
	product, err := store.CreateProduct(ctx, randSku, "rtx-3060", 2500000, 1)
	if err != nil {
		t.Fatalf("Failed to create product:%v", err)
	}
	var domainInput domain.CreateOrderInput
	var OrderItemInput domain.OrderItemInput
	OrderItemInput.ProductID = product.ID
	OrderItemInput.Quantity = 1
	domainInput.CustomerEmail = "test@mail.ru"
	domainInput.Items = []domain.OrderItemInput{}
	domainInput.Items = append(domainInput.Items, OrderItemInput)
	// _, err = store.CreateOrder(ctx, domainInput)
	// if err != nil {
	// 	t.Fatalf("Failed to create order: %v", err)
	// }
	wg := sync.WaitGroup{}
	errChan := make(chan error, 20)
	startChan := make(chan struct{})

	for range 20 {
		wg.Add(+1)
		go func() {
			defer wg.Done()
			<-startChan
			_, err := store.CreateOrder(ctx, domainInput)
			errChan <- err
		}()
	}
	close(startChan)
	wg.Wait()
	close(errChan)
	var nill int
	var ErrInsufficientStockCount int
	var otherError int
	for err := range errChan {
		if err == nil {
			nill += 1
		} else if errors.Is(err, ErrInsufficientStock) {
			ErrInsufficientStockCount += 1
		} else {
			otherError += 1
		}
	}
	if nill != 1 || ErrInsufficientStockCount != 19 {
		t.Fatalf("Not right count error. Nil count = %v; InsufficientCount = %v", nill, ErrInsufficientStockCount)

	}
	if otherError != 0 {
		t.Fatalf("Other error non null:%v", otherError)
	}
	var stock int
	err = pool.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1 `, product.ID).Scan(&stock)
	if err != nil {
		t.Fatalf("Failed to get stock product from db: %v", err)
	}
	if stock != 0 {
		t.Errorf("Stock must be 0 after test:%v", stock)
	}
}
func TestCreateOrderDuplicateItems(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/inventory_test?sslmode=disable")
	if err != nil {
		t.Fatalf("Err to connect test database:%v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Err to ping test database:%v", err)
	}
	store := New(pool)
	randSku := "test" + strconv.Itoa(rand.Int())
	product, err := store.CreateProduct(ctx, randSku, "rtx-3060", 2500000, 10)

	if err != nil {
		t.Fatalf("Failed to create product:%v", err)
	}
	var domainInput domain.CreateOrderInput
	var OrderItemInputFirst domain.OrderItemInput
	var OrderItemInputSecond domain.OrderItemInput
	OrderItemInputFirst.ProductID = product.ID
	OrderItemInputFirst.Quantity = 3
	OrderItemInputSecond.ProductID = product.ID
	OrderItemInputSecond.Quantity = 4
	domainInput.CustomerEmail = "test@mail.ru"
	domainInput.Items = []domain.OrderItemInput{}
	domainInput.Items = append(domainInput.Items, OrderItemInputFirst)
	domainInput.Items = append(domainInput.Items, OrderItemInputSecond)
	order, err := store.CreateOrder(ctx, domainInput)
	if err != nil {
		t.Fatalf("Failed to create order: %v", err)
	}
	if order.Total != 17500000 {
		t.Errorf("Non right total, waited 17 500 000")
	}
	var stock int64
	err = pool.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1`, product.ID).Scan(&stock)
	if err != nil {
		t.Fatalf("Error to select from db:%v", err)
	}
	if stock != 3 {
		t.Fatalf("Not right stock in db")
	}
	orders, err := store.GetOrder(ctx, order.ID)
	if err != nil {
		t.Fatalf("Error in get order:%v", err)
	}
	if len(orders.Items) != 1 {
		t.Fatalf("Must be 1, but :%v", orders)
	}
	if orders.Items[0].Quantity != 7 {
		t.Fatalf("Must be 7, but :%v", orders.Items)
	}
}
func TestConcurrentCreateOrderTwo(t *testing.T) {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/inventory_test?sslmode=disable")
	if err != nil {
		t.Fatalf("Err to connect test database:%v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Err to ping test database:%v", err)
	}
	store := New(pool)
	randSku := "test" + strconv.Itoa(rand.Int())
	secondRandSku := randSku + "-as"
	product, err := store.CreateProduct(ctx, randSku, "rtx-3060", 2500000, 2)
	if err != nil {
		t.Fatalf("Failed to create product:%v", err)
	}
	productSecond, err := store.CreateProduct(ctx, secondRandSku, "rtx-4080", 2500000, 2)

	if err != nil {
		t.Fatalf("Failed to create product:%v", err)
	}
	var domainInput domain.CreateOrderInput
	var SecondDomainInput domain.CreateOrderInput
	var OrderItemInput domain.OrderItemInput
	var SecondOrderItemInput domain.OrderItemInput
	OrderItemInput.ProductID = product.ID
	OrderItemInput.Quantity = 1
	SecondOrderItemInput.ProductID = productSecond.ID
	SecondOrderItemInput.Quantity = 1
	domainInput.CustomerEmail = "test@mail.ru"
	SecondDomainInput.CustomerEmail = "test2@mail.ru"
	domainInput.Items = []domain.OrderItemInput{}
	domainInput.Items = append(domainInput.Items, OrderItemInput, SecondOrderItemInput)
	SecondDomainInput.Items = []domain.OrderItemInput{}
	SecondDomainInput.Items = append(SecondDomainInput.Items, SecondOrderItemInput, OrderItemInput)

	// _, err = store.CreateOrder(ctx, domainInput)
	// if err != nil {
	// 	t.Fatalf("Failed to create order: %v", err)
	// }
	wg := sync.WaitGroup{}
	errChan := make(chan error, 2)
	startChan := make(chan struct{})
	wg.Add(+1)
	go func() {
		defer wg.Done()
		<-startChan
		_, err := store.CreateOrder(ctx, domainInput)
		errChan <- err
	}()
	wg.Add(+1)
	go func() {
		defer wg.Done()
		<-startChan
		_, err := store.CreateOrder(ctx, SecondDomainInput)
		errChan <- err
	}()
	close(startChan)
	wg.Wait()
	close(errChan)
	var nill int
	var ErrInsufficientStockCount int
	var otherError int
	for err := range errChan {
		if err == nil {
			nill += 1
		} else if errors.Is(err, ErrInsufficientStock) {
			ErrInsufficientStockCount += 1
		} else {
			otherError += 1
		}
	}
	if nill != 2 || ErrInsufficientStockCount != 0 {
		t.Fatalf("Not right count error. Nil count = %v; InsufficientCount = %v", nill, ErrInsufficientStockCount)

	}
	if otherError != 0 {
		t.Fatalf("Other error non null:%v", otherError)
	}
	var stock int
	err = pool.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1 `, product.ID).Scan(&stock)
	if err != nil {
		t.Fatalf("Failed to get stock product from db: %v", err)
	}
	if stock != 0 {
		t.Errorf("Stock must be 0 after test:%v", stock)
	}
	var secondStock int
	err = pool.QueryRow(ctx, `SELECT stock FROM products WHERE id = $1 `, productSecond.ID).Scan(&secondStock)
	if err != nil {
		t.Fatalf("Failed to get stock product from db: %v", err)
	}
	if secondStock != 0 {
		t.Errorf("Stock must be 0 after test:%v", secondStock)
	}
}
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

func TestCancelOrderContextCancelledWhileLocked(t *testing.T) {
	pool := testPool(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f := createFixture(t, ctx, pool, []testItem{
		{stock: 8, quantity: 2},
	})

	// Отдельный пул только для проверяемого CancelOrder.
	// Одно соединение позволяет обнаружить утечку:
	// если оно останется занятым, следующий запрос не выполнится.
	cfg := pool.Config()
	cfg.MaxConns = 1
	cfg.MinConns = 0

	workerPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(workerPool.Close)

	var workerPID int32
	err = workerPool.QueryRow(ctx, `SELECT pg_backend_pid()`).
		Scan(&workerPID)
	if err != nil {
		t.Fatal(err)
	}

	s := New(workerPool)

	// Заранее блокируем заказ другой транзакцией.
	blocker, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cleanupCancel()
		_ = blocker.Rollback(cleanupCtx)
	}()

	var blockerPID int32
	err = blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).
		Scan(&blockerPID)
	if err != nil {
		t.Fatal(err)
	}

	var status string
	err = blocker.QueryRow(ctx, `
		SELECT status
		FROM orders
		WHERE id = $1
		FOR UPDATE
	`, f.orderID).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}

	requestCtx, requestCancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		result <- s.CancelOrder(requestCtx, f.orderID)
	}()

	// Даже если проверка упадёт, отменяем вызов и ждём его завершения.
	defer func() {
		requestCancel()
		select {
		case <-done:
		case <-time.After(7 * time.Second):
			t.Error("CancelOrder did not finish during cleanup")
		}
	}()

	// Подтверждаем реальное ожидание блокировки в PostgreSQL.
	waitCtx, waitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer waitCancel()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		var blocked bool
		err := pool.QueryRow(waitCtx, `
			SELECT $1::integer = ANY(pg_blocking_pids($2::integer))
		`, blockerPID, workerPID).Scan(&blocked)
		if err != nil {
			t.Fatalf("observe lock wait: %v", err)
		}
		if blocked {
			break
		}

		select {
		case err := <-result:
			t.Fatalf("CancelOrder returned before waiting for lock: %v", err)
		case <-waitCtx.Done():
			t.Fatal("CancelOrder was not observed waiting for the lock")
		case <-ticker.C:
		}
	}

	// Заказ всё ещё заблокирован. Отменяем именно контекст запроса.
	requestCancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("CancelOrder did not stop after context cancellation")
	}

	checkState(t, ctx, pool, f, "created", []int{8})

	// Проверяем, что единственный слот пула доступен,
	// а полученное соединение не находится внутри транзакции.
	probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer probeCancel()

	conn, err := workerPool.Acquire(probeCtx)
	if err != nil {
		t.Fatalf("pool connection was not released: %v", err)
	}

	txStatus := conn.Conn().PgConn().TxStatus()
	var one int
	probeErr := conn.QueryRow(probeCtx, `SELECT 1`).Scan(&one)
	conn.Release()

	if txStatus != 'I' {
		t.Fatalf("connection transaction status = %q, want idle", txStatus)
	}
	if probeErr != nil {
		t.Fatalf("connection is not usable: %v", probeErr)
	}
	if one != 1 {
		t.Fatalf("SELECT 1 returned %d", one)
	}

	// Снимаем нашу блокировку и проверяем обычную отмену.
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	if err := s.CancelOrder(ctx, f.orderID); err != nil {
		t.Fatalf("cancellation after releasing lock: %v", err)
	}
	checkState(t, ctx, pool, f, "cancelled", []int{10})
}
