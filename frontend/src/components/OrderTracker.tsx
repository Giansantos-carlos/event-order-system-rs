import { useEffect, useState } from "react";
import { getOrder } from "../api";
import type { Order } from "../types";

const TERMINAL_STATUSES = new Set(["CONFIRMED", "CANCELLED"]);
const POLL_INTERVAL_MS = 1200;

interface Props {
  order: Order;
  onSettled?: () => void;
}

const STATUS_LABEL: Record<string, string> = {
  PENDING: "Aguardando pagamento e estoque",
  CONFIRMED: "Confirmado",
  CANCELLED: "Cancelado",
};

export function OrderTracker({ order: initialOrder, onSettled }: Props) {
  const [order, setOrder] = useState(initialOrder);
  const [polling, setPolling] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setOrder(initialOrder);
    setPolling(!TERMINAL_STATUSES.has(initialOrder.status));
    setError(null);
  }, [initialOrder]);

  useEffect(() => {
    if (!polling) return;
    let cancelled = false;

    const interval = setInterval(async () => {
      if (cancelled) return;
      try {
        const fresh = await getOrder(order.id);
        if (cancelled) return;
        setOrder(fresh);
        if (TERMINAL_STATUSES.has(fresh.status)) {
          setPolling(false);
          onSettled?.();
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
    }, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [polling, order.id, onSettled]);

  const statusClass = order.status.toLowerCase();

  return (
    <div className={`order-card status-${statusClass}`}>
      <div className="order-card-header">
        <span className={`status-badge status-${statusClass}`}>
          {polling && <span className="spinner" />}
          {STATUS_LABEL[order.status] ?? order.status}
        </span>
        <span className="order-id">#{order.id.slice(0, 8)}</span>
      </div>

      <dl className="order-meta">
        <dt>Cliente</dt>
        <dd>{order.customerId}</dd>
        <dt>Total</dt>
        <dd>${order.totalAmount}</dd>
        <dt>Criado em</dt>
        <dd>{new Date(order.createdAt).toLocaleTimeString()}</dd>
      </dl>

      <ul className="order-items">
        {order.items.map((item, i) => (
          <li key={i}>
            {item.quantity}x {item.productId} @ ${item.unitPrice}
          </li>
        ))}
      </ul>

      {error && <p className="error">Erro ao atualizar: {error}</p>}
    </div>
  );
}
