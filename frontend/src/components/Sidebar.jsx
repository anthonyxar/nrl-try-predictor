import React, { useEffect, useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'

const STORAGE_KEY = 'nrltp_sidebar_collapsed'

function getInitialCollapsed() {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY)
    if (stored !== null) return stored === '1'
  } catch {
    // localStorage unavailable (private browsing, etc.) — fall through to default
  }
  return false
}

const NAV_ITEMS = [
  {
    to: '/', end: true, label: 'Dashboard',
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <rect x="3" y="12" width="4" height="8" /><rect x="10" y="7" width="4" height="13" /><rect x="17" y="3" width="4" height="17" />
      </svg>
    ),
  },
  {
    to: '/predictions', end: false, label: 'Predictions', matchPrefixes: ['/round'],
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <rect x="3" y="5" width="18" height="16" rx="2" /><path d="M3 10h18M8 3v4M16 3v4" />
      </svg>
    ),
  },
  {
    to: '/teams', end: false, label: 'Team Stats', matchPrefixes: ['/team'],
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M12 3l7 3v5.5c0 4.6-3 8.2-7 9.5-4-1.3-7-4.9-7-9.5V6l7-3z" />
      </svg>
    ),
  },
  {
    to: '/players', end: false, label: 'Player Stats', matchPrefixes: ['/player'],
    icon: (
      <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="12" cy="8" r="4" /><path d="M4 21c0-4 4-6 8-6s8 2 8 6" />
      </svg>
    ),
  },
]

function isNavItemActive(item, location) {
  return (item.matchPrefixes || []).some(p => location.pathname.startsWith(p))
}

function NavItems({ location }) {
  return NAV_ITEMS.map(item => (
    <NavLink
      key={item.to}
      to={item.to}
      end={item.end}
      className={({ isActive }) => `sidebar-link ${isActive || isNavItemActive(item, location) ? 'active' : ''}`}
      title={item.label}
    >
      {item.icon}
      <span className="sidebar-link-label">{item.label}</span>
    </NavLink>
  ))
}

export default function Sidebar() {
  const [collapsed, setCollapsed] = useState(getInitialCollapsed)
  const location = useLocation()

  useEffect(() => {
    try {
      window.localStorage.setItem(STORAGE_KEY, collapsed ? '1' : '0')
    } catch {
      // ignore — nothing we can do if storage is unavailable
    }
  }, [collapsed])

  // Fully collapsed: nothing but a thin strip with an arrow to bring the
  // whole sidebar back — not an icon-only rail, so it actually gives
  // content the space back rather than just hiding the text labels.
  // Desktop/tablet only — hidden below 640px in favour of the top-docked
  // mobile nav (MobileNavTrigger) so it stops eating screen width on phones.
  if (collapsed) {
    return (
      <aside className="sidebar sidebar-desktop collapsed">
        <button
          type="button"
          className="sidebar-expand-btn"
          onClick={() => setCollapsed(false)}
          title="Expand sidebar"
          aria-label="Expand sidebar"
        >
          <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M9 6l6 6-6 6" />
          </svg>
        </button>
      </aside>
    )
  }

  return (
    <aside className="sidebar sidebar-desktop">
      <div className="sidebar-top">
        <span className="sidebar-brand">
          <span className="sidebar-brand-mark">NRL</span>
          <span className="sidebar-brand-text">Try Predictor</span>
        </span>
        <button
          type="button"
          className="sidebar-pin-btn pinned"
          onClick={() => setCollapsed(true)}
          title="Collapse sidebar"
          aria-label="Collapse sidebar"
        >
          <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M15 6l-6 6 6 6" />
          </svg>
        </button>
      </div>

      <nav className="sidebar-nav">
        <NavItems location={location} />
      </nav>
    </aside>
  )
}

// Mobile-only (<640px): a hamburger button docked in the header, in the
// mirror spot from the search trigger (left vs. right), that expands into
// a full-screen nav overlay — same interaction pattern as the mobile
// search overlay in SearchBar.jsx. Rendered by App.jsx inside the header
// so the flex layout puts it opposite the search trigger for free.
export function MobileNavTrigger() {
  const [open, setOpen] = useState(false)
  const location = useLocation()

  useEffect(() => {
    setOpen(false)
  }, [location.pathname])

  return (
    <>
      <button
        type="button"
        className="sidebar-mobile-trigger"
        onClick={() => setOpen(true)}
        aria-label="Open menu"
      >
        <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M3 6h18M3 12h18M3 18h18" />
        </svg>
      </button>

      {open && (
        <div className="sidebar-mobile-overlay">
          <div className="sidebar-mobile-overlay-bar">
            <span className="sidebar-brand">
              <span className="sidebar-brand-mark">NRL</span>
              <span className="sidebar-brand-text">Try Predictor</span>
            </span>
            <button type="button" className="sidebar-mobile-close" onClick={() => setOpen(false)} aria-label="Close menu">
              &times;
            </button>
          </div>
          <nav className="sidebar-mobile-nav">
            <NavItems location={location} />
          </nav>
        </div>
      )}
    </>
  )
}
