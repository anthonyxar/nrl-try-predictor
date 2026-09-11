import React, { useState, useEffect, useRef } from 'react'
import { createPortal } from 'react-dom'
import { useNavigate } from 'react-router-dom'
import { fetchJson } from '../api'

export default function SearchBar({ apiBase }) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState(null)
  const [open, setOpen] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [searchError, setSearchError] = useState(false)
  const wrapRef = useRef(null)
  const mobileInputRef = useRef(null)
  const debounceRef = useRef(null)
  const navigate = useNavigate()

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current)
    if (!query || query.length < 2) { setResults(null); setOpen(false); return }

    setLoading(true)
    setSearchError(false)
    debounceRef.current = setTimeout(() => {
      fetchJson(`${apiBase}/search?q=${encodeURIComponent(query)}`)
        .then(data => { setResults(data); setOpen(true); setLoading(false) })
        .catch(() => { setSearchError(true); setOpen(true); setLoading(false) })
    }, 300)

    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [query, apiBase])

  useEffect(() => {
    if (mobileOpen) mobileInputRef.current?.focus()
  }, [mobileOpen])

  const closeMobile = () => {
    setMobileOpen(false)
    setOpen(false)
    setQuery('')
    setResults(null)
  }

  const handlePlayerClick = (name) => {
    setOpen(false)
    setMobileOpen(false)
    setQuery('')
    navigate(`/player?name=${encodeURIComponent(name)}`)
  }

  const handleTeamClick = (name) => {
    setOpen(false)
    setMobileOpen(false)
    setQuery('')
    navigate(`/team?name=${encodeURIComponent(name)}`)
  }

  const getInitials = (name) =>
    (name || '').split(' ').map(n => n[0] || '').join('').substring(0, 2).toUpperCase()

  const hasResults = results && (results.players?.length > 0 || results.teams?.length > 0)

  // Plain <input>s outside a <form> never get a submit event, so Enter did
  // nothing — jump to the top result instead, same as clicking it.
  const handleKeyDown = (e) => {
    if (e.key !== 'Enter') return
    e.preventDefault()
    const firstTeam = results?.teams?.[0]
    const firstPlayer = results?.players?.[0]
    if (firstTeam) {
      handleTeamClick(typeof firstTeam === 'string' ? firstTeam : firstTeam.name)
    } else if (firstPlayer) {
      handlePlayerClick(firstPlayer.name)
    }
  }

  const resultsBody = (
    <>
      {loading && <div className="search-loading">Searching...</div>}
      {!loading && searchError && (
        <div className="search-empty">Search failed — try again</div>
      )}
      {!loading && !searchError && !hasResults && query.length >= 2 && (
        <div className="search-empty">No results found</div>
      )}
      {results?.teams?.length > 0 && (
        <div className="search-group">
          <div className="search-group-label">Teams</div>
          {results.teams.map(t => {
            const name = typeof t === 'string' ? t : t.name
            const themeKey = typeof t === 'string' ? 'nrl' : (t.theme_key || 'nrl')
            const colour = typeof t === 'string' ? '#333' : (t.colour || '#333')
            return (
              <div key={name} className="search-result team-result" onClick={() => handleTeamClick(name)}>
                <div className="search-result-avatar">
                  <img
                    className="search-team-logo"
                    src={`https://www.nrl.com/.theme/${themeKey}/badge.svg`}
                    alt={name}
                    onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
                  />
                  <div className="search-avatar-fallback" style={{ backgroundColor: colour, display: 'none' }}>
                    {getInitials(name)}
                  </div>
                </div>
                <div className="search-result-text">
                  <span className="search-result-name">{name}</span>
                </div>
              </div>
            )
          })}
        </div>
      )}
      {results?.players?.length > 0 && (
        <div className="search-group">
          <div className="search-group-label">Players</div>
          {results.players.map(p => (
            <div key={p.name} className="search-result player-result" onClick={() => handlePlayerClick(p.name)}>
              <div className="search-result-avatar">
                {p.headshot ? (
                  <img
                    className="search-player-headshot"
                    src={p.headshot}
                    alt={p.name}
                    onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
                  />
                ) : null}
                <div className="search-avatar-fallback" style={{ display: p.headshot ? 'none' : 'flex' }}>
                  {getInitials(p.name)}
                </div>
              </div>
              <div className="search-result-text">
                <span className="search-result-name">{p.name}</span>
                <span className="search-result-meta">{p.position} — {p.team}</span>
                <span className="search-result-stats">{p.total_games} games, {p.total_tries} tries</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </>
  )

  return (
    <>
      {/* Desktop / tablet: inline search bar with a dropdown. Hidden on
          mobile in favour of the icon button + overlay below, where the
          header itself is hidden to save vertical space. */}
      <div className="search-bar-wrap search-bar-desktop" ref={wrapRef}>
        <div className="search-input-wrap">
          <svg className="search-icon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
            <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
          </svg>
          <input
            className="search-input"
            type="text"
            placeholder="Search players or teams..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onFocus={() => { if (hasResults) setOpen(true) }}
            onKeyDown={handleKeyDown}
          />
          {query && (
            <button className="search-clear" onClick={() => { setQuery(''); setResults(null); setOpen(false) }}>
              &times;
            </button>
          )}
        </div>
        {open && <div className="search-dropdown">{resultsBody}</div>}
      </div>

      <button
        type="button"
        className="search-mobile-trigger"
        onClick={() => setMobileOpen(true)}
        aria-label="Search players or teams"
      >
        <svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
        </svg>
      </button>

      {mobileOpen && createPortal(
        // Portalled to <body> — see the matching note in Sidebar.jsx's
        // MobileNavTrigger: the header's `backdrop-filter` makes it a
        // containing block for `position: fixed` descendants, so this
        // overlay would otherwise be squashed into the header's own slim
        // box instead of covering the viewport.
        <div className="search-mobile-overlay">
          <div className="search-mobile-overlay-bar">
            <div className="search-input-wrap">
              <svg className="search-icon" viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="11" cy="11" r="8" /><path d="M21 21l-4.35-4.35" />
              </svg>
              <input
                ref={mobileInputRef}
                className="search-input"
                type="text"
                placeholder="Search players or teams..."
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={handleKeyDown}
              />
              {query && (
                <button className="search-clear" onClick={() => { setQuery(''); setResults(null) }}>
                  &times;
                </button>
              )}
            </div>
            <button type="button" className="search-mobile-close" onClick={closeMobile} aria-label="Close search">
              &times;
            </button>
          </div>
          <div className="search-mobile-results">
            {query.length < 2 ? (
              <div className="search-empty">Type at least 2 characters to search</div>
            ) : resultsBody}
          </div>
        </div>,
        document.body
      )}
    </>
  )
}
