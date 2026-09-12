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

// The app mark, matching public/favicon.svg exactly — same geometry, same
// literal colours (not var(--accent)), so the brand in the rail and the
// icon in the tab can't drift apart. Small-size variant: the single-rise
// lacing, since this renders at 26px.
function BrandMark({ size = 26, className }) {
  return (
    <svg viewBox="0 0 512 512" width={size} height={size} className={className} aria-hidden="true">
      <rect width="512" height="512" rx="115" fill="#16a34a" />
      <g transform="translate(256 256) rotate(-30)">
        <path d="M -170 0 Q -112 -106 0 -106 Q 112 -106 170 0 Q 112 106 0 106 Q -112 106 -170 0 Z" fill="#ffffff" />
        <path d="M -88 34 L -6 4 L 88 -46" fill="none" stroke="#15903f" strokeWidth="34" strokeLinecap="round" strokeLinejoin="round" />
      </g>
    </svg>
  )
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

  // Collapsed: an icon-only rail, not an empty strip — the nav icons stay
  // clickable so collapsing doesn't cost people the ability to navigate.
  // Desktop/tablet only — hidden below 640px in favour of the bottom-docked
  // MobileTabBar so it stops eating screen width on phones.
  return (
    <aside className={`sidebar sidebar-desktop ${collapsed ? 'collapsed' : ''}`}>
      <div className="sidebar-top">
        <span className="sidebar-brand" title="NRL Try Predictor" role="img" aria-label="NRL Try Predictor">
          <BrandMark />
        </span>
        <button
          type="button"
          className={`sidebar-pin-btn ${collapsed ? '' : 'pinned'}`}
          onClick={() => setCollapsed(c => !c)}
          title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <svg
            viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
            style={{ transform: collapsed ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s ease' }}
          >
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

// Mobile-only (<640px): the brand mark docked in the header, in the mirror
// spot from the search trigger (left vs. right), linking home. Now that
// primary nav lives in the bottom-docked MobileTabBar, this is just the
// logo — no menu to open, so no hamburger and no overlay.
export function MobileBrandLink() {
  return (
    <NavLink to="/" end className="sidebar-mobile-trigger" aria-label="NRL Try Predictor home">
      <BrandMark size={22} className="sidebar-mobile-trigger-mark" />
    </NavLink>
  )
}

// Mobile-only (<640px): a bottom-docked tab bar replacing the sidebar as
// primary nav — the standard mobile-app pattern of always-visible tabs
// beats a menu you have to open first. Round/match pages layer their own
// contextual nav-bar just above this one (see .nav-bar.sticky in
// styles.css) rather than competing with it for the same strip.
export function MobileTabBar() {
  const location = useLocation()
  return (
    <nav className="mobile-tabbar">
      {NAV_ITEMS.map(item => (
        <NavLink
          key={item.to}
          to={item.to}
          end={item.end}
          className={({ isActive }) => `mobile-tab ${isActive || isNavItemActive(item, location) ? 'active' : ''}`}
        >
          {item.icon}
          <span className="mobile-tab-label">{item.label}</span>
        </NavLink>
      ))}
    </nav>
  )
}
