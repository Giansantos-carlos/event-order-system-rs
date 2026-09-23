import { useCallback, useEffect, useState } from "react";
import "./App.css";
import { createOrder, getStock, ApiError } from "./api";
import { CreateOrderForm } from "./components/CreateOrderForm";
import { OrderTracker } from "./components/OrderTracker";
import type { Order, OrderItem, StockItem } from "./types";

const STOCK_POLL_INTERVAL_MS = 2000;

function App() {
  const [orders, setOrders] = useState<Order[]>([]);
  const [stock, setStock] = useState<StockItem[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refreshStock = useCallback(async () => {
    try {
      setStock(await getStock());
    } catch {
      // transient failure — keep showing the last known stock levels
    }
  }, []);

  useEffect(() => {
    refreshStock();
    const interval = setInterval(refreshStock, STOCK_POLL_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [refreshStock]);

  async function handleCreateOrder(customerId: string, items: OrderItem[]) {
    setSubmitting(true);
    setError(null);
    try {
      const order = await createOrder({ customerId, items });
      setOrders((prev) => [order, ...prev]);
      refreshStock();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : "Falha ao criar pedido");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="app">
      <header>
        <h1>Event Order System</h1>
        <p className="subtitle">
          Cria pedidos via <code>order-service</code> e acompanha o andamento da saga
          (pagamento + estoque) em tempo real.
        </p>
      </header>

      <main>
        <section className="panel">
          <h2>Novo pedido</h2>
          <CreateOrderForm onSubmit={handleCreateOrder} submitting={submitting} stock={stock} />
          {error && <p className="error">{error}</p>}
        </section>

        <section className="panel">
          <h2>Pedidos</h2>
          {orders.length === 0 && <p className="empty">Nenhum pedido criado ainda.</p>}
          <div className="orders-list">
            {orders.map((order) => (
              <OrderTracker key={order.id} order={order} onSettled={refreshStock} />
            ))}
          </div>
        </section>
      </main>
    </div>
  );
}

export default App;
