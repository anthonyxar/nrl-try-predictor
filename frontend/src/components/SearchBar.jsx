import React, { useState, useEffect, useRef } from 'react'
import { useNavigate } from 'react-router-dom'

export default function SearchBar({ apiBase }) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState(null)
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const wrapRef = useRef(null)
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
    debounceRef.current = setTimeout(() => {
      fetch(`${apiBase}/search?q=${encodeURIComponent(query)}`)
        .then(r => r.json())
        .then(data => { setResults(data); setOpen(true); setLoading(false) })
        .catch(() => setLoading(false))
    }, 300)

    return () => { if (debounceRef.current) clearTimeout(debounceRef.current) }
  }, [query, apiBase])

  const handlePlayerClick = (name) => {
    setOpen(false)
    setQuery('')
    navigate(`/player?name=${encodeURIComponent(name)}`)
  }

  const handleTeamClick = (name) => {
    setOpen(false)
    setQuery('')
    navigate(`/team?name=${encodeURIComponent(name)}`)
  }

  const hasResults = results && (results.players?.length > 0 || results.teams?.length > 0)

  return (
    <div className="search-bar-wrap" ref={wrapRef}>
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
        />
        {query && (
          <button className="search-clear" onClick={() => { setQuery(''); setResults(null); setOpen(false) }}>
            &times;
          </button>
        )}
      </div>
      {open && (
        <div className="search-dropdown">
          {loading && <div className="search-loading">Searching...</div>}
          {!loading && !hasResults && query.length >= 2 && (
            <div className="search-empty">No results found</div>
          )}
          {results?.teams?.length > 0 && (
            <div className="search-group">
              <div className="search-group-label">Teams</div>
              {results.teams.map(name => (
                <div key={name} className="search-result team-result" onClick={() => handleTeamClick(name)}>
                  <span className="search-result-name">{name}</span>
                </div>
              ))}
            </div>
          )}
          {results?.players?.length > 0 && (
            <div className="search-group">
              <div className="search-group-label">Players</div>
              {results.players.map(p => (
                <div key={p.name} className="search-result player-result" onClick={() => handlePlayerClick(p.name)}>
                  <span className="search-result-name">{p.name}</span>
                  <span className="search-result-meta">{p.position} — {p.team}</span>
                  <span className="search-result-stats">{p.total_games} games, {p.total_tries} tries</span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
