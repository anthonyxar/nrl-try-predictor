import React, { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import { fetchJson } from '../api'

const SORT_FIELDS = {
  name: p => p.name || '',
  position: p => p.position || '',
  team: p => p.team || '',
  total_games: p => p.total_games || 0,
  total_tries: p => p.total_tries || 0,
}

export default function PlayersList({ apiBase }) {
  const [players, setPlayers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [query, setQuery] = useState('')
  const [position, setPosition] = useState('all')
  const [team, setTeam] = useState('all')
  const [sortKey, setSortKey] = useState('name')
  const [sortDir, setSortDir] = useState('asc')
  const [retryCount, setRetryCount] = useState(0)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchJson(`${apiBase}/players`)
      .then(data => { if (!cancelled) { setPlayers(data); setLoading(false) } })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })
    return () => { cancelled = true }
  }, [apiBase, retryCount])

  const positions = useMemo(() => {
    const set = new Set(players.map(p => p.position).filter(Boolean))
    return Array.from(set).sort()
  }, [players])

  const teams = useMemo(() => {
    const set = new Set(players.map(p => p.team).filter(Boolean))
    return Array.from(set).sort()
  }, [players])

  const handleSort = (key) => {
    if (sortKey === key) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    const rows = players.filter(p => {
      if (position !== 'all' && p.position !== position) return false
      if (team !== 'all' && p.team !== team) return false
      if (q && !p.name.toLowerCase().includes(q)) return false
      return true
    })

    const getValue = SORT_FIELDS[sortKey] || SORT_FIELDS.name
    const dir = sortDir === 'asc' ? 1 : -1
    return rows.sort((a, b) => {
      const av = getValue(a)
      const bv = getValue(b)
      if (typeof av === 'string' || typeof bv === 'string') {
        return String(av).localeCompare(String(bv)) * dir
      }
      return (av - bv) * dir
    })
  }, [players, query, position, team, sortKey, sortDir])

  if (loading) return <LoadingSpinner text="Loading players..." />
  if (error) return (
    <div className="error-container">
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
    </div>
  )

  return (
    <div className="players-list">
      <div className="list-page-header">
        <h2>Player Stats</h2>
        <div className="players-filters">
          <input
            className="players-search-input"
            type="text"
            placeholder="Filter by name..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <select className="season-select" value={team} onChange={(e) => setTeam(e.target.value)}>
            <option value="all">All Teams</option>
            {teams.map(t => <option key={t} value={t}>{t}</option>)}
          </select>
          <select className="season-select" value={position} onChange={(e) => setPosition(e.target.value)}>
            <option value="all">All Positions</option>
            {positions.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
      </div>

      <div className="players-table">
        <div className="roster-row roster-heading">
          <SortHeader className="roster-name" label="Player" sortKey="name" activeKey={sortKey} dir={sortDir} onClick={handleSort} />
          <SortHeader className="roster-pos" label="Position" sortKey="position" activeKey={sortKey} dir={sortDir} onClick={handleSort} />
          <SortHeader className="players-team" label="Team" sortKey="team" activeKey={sortKey} dir={sortDir} onClick={handleSort} />
          <SortHeader className="roster-games" label="Games" sortKey="total_games" activeKey={sortKey} dir={sortDir} onClick={handleSort} />
          <SortHeader className="roster-tries" label="Tries" sortKey="total_tries" activeKey={sortKey} dir={sortDir} onClick={handleSort} />
        </div>
        {filtered.map(p => (
          <div key={p.name} className="roster-row clickable"
            onClick={() => navigate(`/player?name=${encodeURIComponent(p.name)}`)}>
            <span className="roster-name">{p.name}</span>
            <span className="roster-pos">{p.position || '—'}</span>
            <span className="players-team">{p.team || '—'}</span>
            <span className="roster-games">{p.total_games}</span>
            <span className="roster-tries">{p.total_tries > 0 ? p.total_tries : '—'}</span>
          </div>
        ))}
      </div>

      {filtered.length === 0 && (
        <div className="no-games">No players match this filter</div>
      )}
    </div>
  )
}

function SortHeader({ className, label, sortKey, activeKey, dir, onClick }) {
  const active = sortKey === activeKey
  return (
    <span
      className={`${className} sortable-header ${active ? 'active' : ''}`}
      onClick={() => onClick(sortKey)}
      role="button"
      tabIndex={0}
      aria-sort={active ? (dir === 'asc' ? 'ascending' : 'descending') : 'none'}
    >
      {label}
      <span className="sort-arrow">{active ? (dir === 'asc' ? '▲' : '▼') : ''}</span>
    </span>
  )
}
