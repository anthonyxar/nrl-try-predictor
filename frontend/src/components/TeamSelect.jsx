import React, { useEffect, useRef, useState } from 'react'

// A team picker with each team's logo shown next to its name — native
// <select>/<option> elements can't render images, so this is a small
// custom dropdown instead, reusing the same open/close-on-outside-click
// pattern as SearchBar's results dropdown.
export default function TeamSelect({ teams, value, onChange, placeholder = 'Select a team', includeAllOption = false, allLabel = 'All Teams' }) {
  const [open, setOpen] = useState(false)
  const wrapRef = useRef(null)

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (wrapRef.current && !wrapRef.current.contains(e.target)) setOpen(false)
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const selected = teams.find(t => t.name === value)

  const handleSelect = (name) => {
    onChange(name)
    setOpen(false)
  }

  return (
    <div className="team-select-wrap" ref={wrapRef}>
      <button type="button" className="team-select-trigger" onClick={() => setOpen(o => !o)} aria-haspopup="listbox" aria-expanded={open}>
        {selected ? (
          <>
            <img
              className="team-select-trigger-logo"
              src={`https://www.nrl.com/.theme/${selected.theme_key || 'nrl'}/badge.svg`}
              alt=""
              onError={(e) => { e.target.style.display = 'none' }}
            />
            <span>{selected.name}</span>
          </>
        ) : (
          <span className="team-select-placeholder">{includeAllOption ? allLabel : placeholder}</span>
        )}
        <svg className="team-select-chevron" viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
          <path d="M6 9l6 6 6-6" />
        </svg>
      </button>
      {open && (
        <div className="team-select-dropdown" role="listbox">
          {includeAllOption && (
            <div
              className={`search-result team-select-option ${!value ? 'active' : ''}`}
              role="option"
              aria-selected={!value}
              onClick={() => handleSelect('')}
            >
              <div className="search-result-text"><span className="search-result-name">{allLabel}</span></div>
            </div>
          )}
          {teams.map(t => (
            <div
              key={t.name}
              className={`search-result team-select-option ${value === t.name ? 'active' : ''}`}
              role="option"
              aria-selected={value === t.name}
              onClick={() => handleSelect(t.name)}
            >
              <div className="search-result-avatar">
                <img
                  className="search-team-logo"
                  src={`https://www.nrl.com/.theme/${t.theme_key || 'nrl'}/badge.svg`}
                  alt={t.name}
                  onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
                />
                <div className="search-avatar-fallback" style={{ backgroundColor: t.colour, display: 'none' }}>
                  {t.name.substring(0, 2).toUpperCase()}
                </div>
              </div>
              <div className="search-result-text">
                <span className="search-result-name">{t.name}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
