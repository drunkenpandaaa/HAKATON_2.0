package store

import (
	"context"
	"errors"
	"fmt"
	"time"

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

	order, err := s.db.Exec(ctx, `
		INSERT INTO orders (customer_email, status, total_cents)
		VALUES ($1, 'created', 0)
	`, input.CustomerEmail)
	_ = order
	if err != nil {
		return domain.Order{}, err
	}

	var result domain.Order
	err = s.db.QueryRow(ctx, `
		SELECT id, customer_email, status, total_cents, created_at
		FROM orders WHERE customer_email = $1
		ORDER BY id DESC LIMIT 1
	`, input.CustomerEmail).Scan(&result.ID, &result.CustomerEmail, &result.Status, &result.Total, &result.CreatedAt)
	if err != nil {
		return domain.Order{}, err
	}

	var total int64
	for _, item := range input.Items {
		if item.Quantity <= 0 {
			return domain.Order{}, ErrInvalidInput
		}

		var name string
		var price int64
		var stock int
		err := s.db.QueryRow(ctx, `
			SELECT name, price_cents, stock FROM products WHERE id = $1
		`, item.ProductID).Scan(&name, &price, &stock)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Order{}, ErrNotFound
		}
		if err != nil {
			return domain.Order{}, err
		}
		if stock < item.Quantity {
			return domain.Order{}, ErrInsufficientStock
		}

		_, err = s.db.Exec(ctx, `UPDATE products SET stock = $1 WHERE id = $2`,
			stock-item.Quantity, item.ProductID)
		if err != nil {
			return domain.Order{}, err
		}

		_, err = s.db.Exec(ctx, `
			INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4, $5)
		`, result.ID, item.ProductID, name, item.Quantity, price)
		if err != nil {
			return domain.Order{}, err
		}
		total += price * int64(item.Quantity)
	}

	_, err = s.db.Exec(ctx, `UPDATE orders SET total_cents = $1 WHERE id = $2`, total, result.ID)
	if err != nil {
		return domain.Order{}, err
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
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.ReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("begin cancel order: %w", err)
	}

	// При любом выходе завершаем незавершённую транзакцию.
	// Используем отдельный контекст: контекст запроса мог отмениться.
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()

	var status string
	err = tx.QueryRow(ctx, `
		SELECT status
		FROM orders
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("lock order: %w", err)
	}

	if status == "cancelled" {
		return nil
	}
	if status != "created" {
		return fmt.Errorf("unexpected order status: %q", status)
	}

	rows, err := tx.Query(ctx, `
		SELECT product_id, quantity
		FROM order_items
		WHERE order_id = $1
		ORDER BY product_id, id
	`, id)
	if err != nil {
		return fmt.Errorf("read order items: %w", err)
	}

	type line struct {
		productID int64
		quantity  int
	}

	var lines []line
	for rows.Next() {
		var l line
		if err := rows.Scan(&l.productID, &l.quantity); err != nil {
			rows.Close()
			return fmt.Errorf("scan order item: %w", err)
		}
		lines = append(lines, l)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate order items: %w", err)
	}

	for _, l := range lines {
		if l.quantity <= 0 {
			return fmt.Errorf("invalid stored quantity for product %d", l.productID)
		}

		tag, err := tx.Exec(ctx, `
			UPDATE products
			SET stock = stock + $1
			WHERE id = $2
		`, l.quantity, l.productID)
		if err != nil {
			return fmt.Errorf("restore product %d stock: %w", l.productID, err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("product %d missing during cancellation", l.productID)
		}
	}

	_, err = tx.Exec(ctx, `
		UPDATE orders
		SET status = 'cancelled'
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit cancel order: %w", err)
	}

	return nil
}
