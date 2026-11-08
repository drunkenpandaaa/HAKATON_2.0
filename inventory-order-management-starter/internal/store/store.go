package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/hackathon/inventory-order-management/internal/domain"
)

var ErrNotFound = errors.New("not found")
var ErrInsufficientStock = errors.New("insufficient stock")
var ErrInvalidInput = errors.New("invalid input")

type Store struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

func (s *Store) CreateProduct(ctx context.Context, sku, name string, price int64, stock int) (domain.Product, error) {
	var p domain.Product
	err := s.db.QueryRow(ctx, `
		INSERT INTO products (sku, name, price_cents, stock)
		VALUES ($1, $2, $3, $4)
		RETURNING id, sku, name, price_cents, stock, created_at
	`, sku, name, price, stock).Scan(&p.ID, &p.SKU, &p.Name, &p.Price, &p.Stock, &p.CreatedAt)
	return p, err
}

func (s *Store) ListProducts(ctx context.Context) ([]domain.Product, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, sku, name, price_cents, stock, created_at
		FROM products ORDER BY id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	products := make([]domain.Product, 0)
	for rows.Next() {
		var p domain.Product
		if err := rows.Scan(&p.ID, &p.SKU, &p.Name, &p.Price, &p.Stock, &p.CreatedAt); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, rows.Err()
}


func (s *Store) CreateOrder(ctx context.Context, input domain.CreateOrderInput) (domain.Order, error) {
	if input.CustomerEmail == "" || len(input.Items) == 0 {
		return domain.Order{}, ErrInvalidInput
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return domain.Order{}, fmt.Errorf("Failed to begin sql query: %w", err)
	}
	defer tx.Rollback(ctx)
	var result domain.Order
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (customer_email, status, total_cents)
		VALUES ($1, 'created', 0)
		RETURNING id, customer_email, status, total_cents, created_at
		`, input.CustomerEmail).Scan(&result.ID, &result.CustomerEmail, &result.Status, &result.Total, &result.CreatedAt)
	if err != nil {
		return domain.Order{}, err
	}
	var total int64
	mapp := make(map[int64]int)
	for _, value := range input.Items {
		if value.Quantity <= 0 || value.ProductID <= 0 {
			return domain.Order{}, ErrInvalidInput
		}
		if math.MaxInt-mapp[value.ProductID] >= value.Quantity {
			mapp[value.ProductID] += value.Quantity
		} else {
			return domain.Order{}, fmt.Errorf("Max quantity for product: %w", ErrInvalidInput)
		}
	}
	copyInputItems := make([]domain.OrderItemInput, 0)
	for k, v := range mapp {
		orderItem := domain.OrderItemInput{
			ProductID: k,
			Quantity:  v,
		}
		copyInputItems = append(copyInputItems, orderItem)
	}
	slices.SortFunc(copyInputItems, func(a domain.OrderItemInput, b domain.OrderItemInput) int {
		if a.ProductID < b.ProductID {
			return -1
		} else if a.ProductID == b.ProductID {
			return 0
		} else {
			return 1
		}
	})
	for _, item := range copyInputItems {
		var name string
		var price int64
		var stock int
		err := tx.QueryRow(ctx, `UPDATE products SET stock = stock - $1 WHERE id = $2 AND stock >= $1 RETURNING name, price_cents, stock`,
			item.Quantity, item.ProductID).Scan(&name, &price, &stock)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				var boolError bool
				err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT id FROM products WHERE id = $1)`, item.ProductID).Scan(&boolError)
				if err != nil {
					return domain.Order{}, fmt.Errorf("Error in exist from db: %w", err)
				}
				if boolError {
					return domain.Order{}, fmt.Errorf("ErrInsufficientStock:%w", ErrInsufficientStock)
				} else {
					return domain.Order{}, fmt.Errorf("Not found in db: %w", ErrNotFound)
				}
			}
			return domain.Order{}, fmt.Errorf("Failed to connect db: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4, $5)
		`, result.ID, item.ProductID, name, item.Quantity, price)
		if err != nil {
			return domain.Order{}, err
		}
		if price <= 0 {
			return domain.Order{}, fmt.Errorf("price must be positive: %w", ErrInvalidInput)
		} else {
			if math.MaxInt64/price < int64(item.Quantity) {
				return domain.Order{}, fmt.Errorf("Failed to sum total, it's bigger than max Int:%w", ErrInvalidInput)
			}
		}
		if math.MaxInt64-total >= price*int64(item.Quantity) {
			total += price * int64(item.Quantity)
		} else {
			return domain.Order{}, fmt.Errorf("Failed to sum total, it's bigger than max Int:%w", ErrInvalidInput)
		}

	}

	_, err = tx.Exec(ctx, `UPDATE orders SET total_cents = $1 WHERE id = $2`, total, result.ID)
	if err != nil {
		return domain.Order{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Order{}, fmt.Errorf("Failed to commit query: %w", err)
	}
	result.Total = total
	return result, nil
}

func (s *Store) GetOrder(ctx context.Context, id int64) (domain.Order, error) {
	var order domain.Order
	err := s.db.QueryRow(ctx, `
		SELECT id, customer_email, status, total_cents, created_at
		FROM orders WHERE id = $1
	`, id).Scan(&order.ID, &order.CustomerEmail, &order.Status, &order.Total, &order.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Order{}, ErrNotFound
	}
	if err != nil {
		return domain.Order{}, err
	}

	rows, err := s.db.Query(ctx, `
		SELECT product_id, product_name, quantity, unit_price_cents
		FROM order_items WHERE order_id = $1 ORDER BY id
	`, id)
	if err != nil {
		return domain.Order{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var item domain.OrderItem
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity, &item.UnitPrice); err != nil {
			return domain.Order{}, err
		}
		order.Items = append(order.Items, item)
	}
	return order, rows.Err()
}

func (s *Store) CancelOrder(ctx context.Context, id int64) error {
	var status string
	err := s.db.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if status == "cancelled" {
		return nil
	}

	rows, err := s.db.Query(ctx, `
		SELECT product_id, quantity FROM order_items WHERE order_id = $1
	`, id)
	if err != nil {
		return err
	}
	type line struct { productID int64; quantity int }
	var lines []line
	for rows.Next() {
		var l line
		if err := rows.Scan(&l.productID, &l.quantity); err != nil {
			rows.Close()
			return err
		}
		lines = append(lines, l)
	}
	rows.Close()

	for _, l := range lines {
		_, err := s.db.Exec(ctx, `
			UPDATE products SET stock = stock + $1 WHERE id = $2
		`, l.quantity, l.productID)
		if err != nil {
			return err
		}
	}
	_, err = s.db.Exec(ctx, `UPDATE orders SET status = 'cancelled' WHERE id = $1`, id)
	return err
}
