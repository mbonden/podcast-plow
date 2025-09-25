import { Link, NavLink, Outlet } from "react-router-dom";
import { cn } from "./lib/utils";

const navItems = [
  { to: "/", label: "Home" },
  { to: "/topics", label: "Topics" }
];

export default function App() {
  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="border-b">
        <div className="container flex items-center justify-between py-4">
          <Link to="/" className="text-lg font-semibold text-primary">
            Podcast Plow
          </Link>
          <nav className="flex items-center gap-6 text-sm font-medium">
            {navItems.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  cn(
                    "transition-colors hover:text-primary",
                    isActive ? "text-primary" : "text-muted-foreground"
                  )
                }
                end={item.to === "/"}
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
        </div>
      </header>
      <main className="container py-8">
        <Outlet />
      </main>
      <footer className="border-t">
        <div className="container py-6 text-sm text-muted-foreground">
          Built with FastAPI + Vite + shadcn/ui.
        </div>
      </footer>
    </div>
  );
}
