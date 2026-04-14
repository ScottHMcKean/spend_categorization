import { QueryClient } from "@tanstack/react-query";
import { createRootRouteWithContext, Link, Outlet, useRouterState } from "@tanstack/react-router";
import { Toaster } from "sonner";

const navLinks = [
  { to: "/", label: "Overview" },
  { to: "/classifications", label: "Classifications" },
  { to: "/review", label: "Spend Review" },
  { to: "/analytics", label: "Analytics" },
  { to: "/comparison", label: "Platform" },
] as const;

function RootLayout() {
  const routerState = useRouterState();
  const currentPath = routerState.location.pathname;

  return (
    <div className="min-h-screen bg-white">
      <header className="border-b border-[#E5EBF0] bg-white sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-6 h-14 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-3">
            <img src="/databricks_logo.svg" alt="Databricks" className="h-5" />
            <span className="text-sm font-semibold text-[#8CA0AC]">|</span>
            <span className="text-sm font-semibold text-[#0B2026] tracking-tight">
              Procurement Accelerator
            </span>
          </Link>
          <nav className="flex items-center gap-6">
            {navLinks.map(({ to, label }) => (
              <Link
                key={to}
                to={to}
                className={`text-sm font-semibold transition-colors ${
                  currentPath === to || (to !== "/" && currentPath.startsWith(to))
                    ? "text-[#FF3621]"
                    : "text-[#0B2026] hover:text-[#FF3621]"
                }`}
              >
                {label}
              </Link>
            ))}
          </nav>
        </div>
      </header>
      <main>
        <Outlet />
      </main>
      <Toaster richColors />
    </div>
  );
}

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient;
}>()({
  component: RootLayout,
});
