import type { CreateOrderRequest, Order, StockItem } from "./types";

const ORDER_BASE_URL = import.meta.env.VITE_ORDER_SERVICE_URL ?? "http://localhost:8081";
const INVENTORY_BASE_URL = import.meta.env.VITE_INVENTORY_SERVICE_URL ?? "http://localhost:8083";

export class ApiError extends Error {}

export async function createOrder(request: CreateOrderRequest): Promise<Order> {
  const res = await fetch(`${ORDER_BASE_URL}/orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
  });
  if (!res.ok) {
    const body = await res.text();
    throw new ApiError(body || `Request failed with status ${res.status}`);
  }
  return res.json();
}

export async function getOrder(id: string): Promise<Order> {
  const res = await fetch(`${ORDER_BASE_URL}/orders/${id}`);
  if (!res.ok) {
    throw new ApiError(`Request failed with status ${res.status}`);
  }
  return res.json();
}

export async function getStock(): Promise<StockItem[]> {
  const res = await fetch(`${INVENTORY_BASE_URL}/stock`);
  if (!res.ok) {
    throw new ApiError(`Request failed with status ${res.status}`);
  }
  return res.json();
}
