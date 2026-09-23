export interface OrderItem {
  productId: string;
  quantity: number;
  unitPrice: number;
}

export interface OrderItemResponse {
  productId: string;
  quantity: number;
  unitPrice: string;
}

export interface Order {
  id: string;
  customerId: string;
  status: "PENDING" | "CONFIRMED" | "CANCELLED" | string;
  totalAmount: string;
  createdAt: string;
  items: OrderItemResponse[];
}

export interface CreateOrderRequest {
  customerId: string;
  items: OrderItem[];
}

export interface StockItem {
  productId: string;
  availableQty: number;
}
