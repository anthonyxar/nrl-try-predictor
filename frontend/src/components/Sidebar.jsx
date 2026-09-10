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
  return typeof window !== 'undefined' && window.innerWidth < 640
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

  return (
    <aside className={`sidebar ${collapsed ? 'collapsed' : ''}`}>
      <div className="sidebar-top">
        <span className="sidebar-brand">
          <span className="sidebar-brand-mark">NRL</span>
          <span className="sidebar-brand-text">Try Predictor</span>
        </span>
        <button
          type="button"
          className={`sidebar-pin-btn ${collapsed ? '' : 'pinned'}`}
          onClick={() => setCollapsed(c => !c)}
          title={collapsed ? 'Pin sidebar open' : 'Collapse sidebar'}
          aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
               style={{ transform: collapsed ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s ease' }}>
            <path d="M15 6l-6 6 6 6" />
          </svg>
        </button>
      </div>

      <nav className="sidebar-nav">
        {NAV_ITEMS.map(item => {
          const extraActive = (item.matchPrefixes || []).some(p => location.pathname.startsWith(p))
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) => `sidebar-link ${isActive || extraActive ? 'active' : ''}`}
              title={item.label}
            >
              {item.icon}
              <span className="sidebar-link-label">{item.label}</span>
            </NavLink>
          )
        })}
      </nav>

      <div className="sidebar-footer">
        <NavLink
          to="/models"
          className={({ isActive }) => `sidebar-link ${isActive ? 'active' : ''}`}
          title="Models"
        >
          <svg viewBox="0 0 24 24" width="20" height="20" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M4 6h16M4 12h16M4 18h16" />
            <circle cx="8" cy="6" r="1.4" fill="currentColor" stroke="none" />
            <circle cx="16" cy="12" r="1.4" fill="currentColor" stroke="none" />
            <circle cx="10" cy="18" r="1.4" fill="currentColor" stroke="none" />
          </svg>
          <span className="sidebar-link-label">Models</span>
        </NavLink>
      </div>
    </aside>
  )
}
