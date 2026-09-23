import { useState } from "react";
import type { OrderItem, StockItem } from "../types";

const DEFAULT_PRICES: Record<string, number> = {
  "PROD-1": 19.99,
  "PROD-2": 29.99,
  "PROD-3": 9.99,
};

interface Props {
  onSubmit: (customerId: string, items: OrderItem[]) => void;
  submitting: boolean;
  stock: StockItem[];
}

function defaultPrice(productId: string): number {
  return DEFAULT_PRICES[productId] ?? 9.99;
}

function emptyItem(stock: StockItem[]): OrderItem {
  const productId = stock[0]?.productId ?? "PROD-1";
  return { productId, quantity: 1, unitPrice: defaultPrice(productId) };
}

export function CreateOrderForm({ onSubmit, submitting, stock }: Props) {
  const [customerId, setCustomerId] = useState("cust-1");
  const [items, setItems] = useState<OrderItem[]>([emptyItem(stock)]);

  function updateItem(index: number, patch: Partial<OrderItem>) {
    setItems((prev) => prev.map((it, i) => (i === index ? { ...it, ...patch } : it)));
  }

  function addItem() {
    setItems((prev) => [...prev, emptyItem(stock)]);
  }

  function removeItem(index: number) {
    setItems((prev) => prev.filter((_, i) => i !== index));
  }

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    onSubmit(customerId, items);
  }

  return (
    <form className="order-form" onSubmit={handleSubmit}>
      <label>
        Customer ID
        <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required />
      </label>

      <div className="items">
        {items.map((item, index) => (
          <div className="item-row" key={index}>
            <select
              value={item.productId}
              onChange={(e) => {
                updateItem(index, {
                  productId: e.target.value,
                  unitPrice: defaultPrice(e.target.value),
                });
              }}
            >
              {stock.length === 0 && <option value={item.productId}>Carregando estoque...</option>}
              {stock.map((s) => (
                <option key={s.productId} value={s.productId} disabled={s.availableQty <= 0}>
                  {s.productId} ({s.availableQty} em estoque)
                </option>
              ))}
            </select>
            <input
              type="number"
              min={1}
              value={item.quantity}
              onChange={(e) => updateItem(index, { quantity: Number(e.target.value) })}
              aria-label="quantity"
            />
            <input
              type="number"
              min={0.01}
              step={0.01}
              value={item.unitPrice}
              onChange={(e) => updateItem(index, { unitPrice: Number(e.target.value) })}
              aria-label="unit price"
            />
            <button
              type="button"
              className="ghost remove-item"
              onClick={() => removeItem(index)}
              disabled={items.length === 1}
              aria-label="Remover item"
              title="Remover item"
            >
              ×
            </button>
          </div>
        ))}
      </div>

      <div className="form-actions">
        <button type="button" className="ghost" onClick={addItem}>
          + Item
        </button>
        <button type="submit" disabled={submitting}>
          {submitting ? "Criando..." : "Criar pedido"}
        </button>
      </div>
    </form>
  );
}
