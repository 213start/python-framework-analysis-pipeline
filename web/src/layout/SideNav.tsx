import { NavLink } from "react-router-dom";
import { navRoutes } from "../routes";

export function SideNav() {
  return (
    <nav className="side-nav" aria-label="主导航">
      <div className="side-nav__brand">Framework Analysis</div>
      <div className="side-nav__links">
        {navRoutes.map((route) => (
          <NavLink
            key={route.path}
            to={route.path}
            className="side-nav__link"
            end={route.path === "/"}
          >
            {route.label}
          </NavLink>
        ))}
      </div>
    </nav>
  );
}
