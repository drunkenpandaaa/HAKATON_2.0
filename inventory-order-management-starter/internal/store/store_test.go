package store

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/hackathon/inventory-order-management/internal/domain"
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
	secondRandSku := randSku+"-as"
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
