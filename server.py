import React, { useState, useCallback, useEffect } from 'react';
import {
  View, Text, FlatList, TouchableOpacity, Modal,
  TextInput, ActivityIndicator, StyleSheet, SafeAreaView,
  ScrollView, StatusBar,
} from 'react-native';

// ── API URL ───────────────────────────────────────────────────────────────────
const DEFAULT_API_URL = 'https://web-production-0e482.up.railway.app';

// ── Theme ─────────────────────────────────────────────────────────────────────
const C = {
  bg:        '#0D0F14',
  surface:   '#161920',
  card:      '#1C2030',
  border:    '#252A3A',
  accent:    '#F0B429',
  accentDim: '#7A5C14',
  green:     '#3DDC84',
  red:       '#FF6B6B',
  orange:    '#FF9500',
  blue:      '#4A9EFF',
  text:      '#F0F2F8',
  muted:     '#6B7280',
  sub:       '#9CA3AF',
};

// ── Helpers ───────────────────────────────────────────────────────────────────
const scoreColor = (score) => {
  if (score >= 3.0) return '#00BFFF';  // electric blue
  if (score >= 2.0) return '#3DDC84';  // green
  if (score >= 1.0) return '#F0B429';  // yellow
  return '#FF6B6B';                    // red
};

const shortLeague = (l) => ({
  'Premier League': 'PL', 'Championship': 'CH',
  'La Liga': 'ESP', 'Serie A': 'ITA',
  'Bundesliga': 'GER', 'Ligue 1': 'FRA',
}[l] || l?.slice(0, 3).toUpperCase());

const formatKickoff = (utcTime) => {
  if (!utcTime) return '';
  try {
    const d = new Date(utcTime);
    const now = new Date();
    const isToday = d.toDateString() === now.toDateString();
    const tomorrow = new Date(now); tomorrow.setDate(now.getDate() + 1);
    const isTomorrow = d.toDateString() === tomorrow.toDateString();
    const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (isToday)    return `Today ${time}`;
    if (isTomorrow) return `Tomorrow ${time}`;
    return `${d.toLocaleDateString([], { weekday: 'short', month: 'short', day: 'numeric' })} ${time}`;
  } catch {
    return utcTime;
  }
};

const formDot = (result) => {
  const colors = { w: C.green, d: C.orange, l: C.red };
  return colors[result?.toLowerCase()] || C.muted;
};

// ── Form Pills ────────────────────────────────────────────────────────────────
const FormPills = ({ form }) => {
  if (!form || form.length === 0) return null;
  return (
    <View style={styles.formRow}>
      {form.map((r, i) => (
        <View key={i} style={[styles.formDot, { backgroundColor: formDot(r) }]} />
      ))}
    </View>
  );
};

// ── Stat Row ──────────────────────────────────────────────────────────────────
const StatRow = ({ label, value, highlight }) => (
  <View style={styles.statRow}>
    <Text style={styles.statLabel}>{label}</Text>
    <Text style={[styles.statValue, highlight && { color: C.accent, fontWeight: '700' }]}>
      {value}
    </Text>
  </View>
);

// ── Player Detail Modal ───────────────────────────────────────────────────────
const PlayerModal = ({ player, onClose }) => {
  if (!player) return null;
  const gap = player.xa_gap >= 0 ? `+${player.xa_gap}` : `${player.xa_gap}`;
  return (
    <Modal transparent animationType="slide" onRequestClose={onClose}>
      <View style={styles.modalOverlay}>
        <View style={styles.modalCard}>
          <View style={styles.modalHeader}>
            <View style={{ flex: 1, marginRight: 12 }}>
              <Text style={styles.modalName}>{player.player}</Text>
              <Text style={styles.modalSub}>
                {player.team}  ·  {shortLeague(player.league)}
              </Text>
              {player.form && player.form.length > 0 && (
                <View style={{ marginTop: 6 }}>
                  <FormPills form={player.form} />
                </View>
              )}
            </View>
            <View style={styles.scoreBadgeLarge}>
              <Text style={[styles.scoreBadgeText, { color: scoreColor(player.score) }]}>
                {player.score.toFixed(2)}
              </Text>
            </View>
          </View>

          {/* Next opponent */}
          {player.next_opponent && (
            <View style={[styles.nextOppRow,
              player.weak_opp_def && styles.nextOppRowWeak]}>
              <Text style={styles.nextOppLabel}>Next: </Text>
              <Text style={styles.nextOppTeam}>{player.next_opponent}</Text>
              {player.weak_opp_def && (
                <Text style={styles.weakBadge}> 🛡️ Weak</Text>
              )}
              {player.next_kickoff && (
                <Text style={styles.nextOppTime}>
                  {' · '}{formatKickoff(player.next_kickoff)}
                </Text>
              )}
            </View>
          )}

          <View style={styles.divider} />

          <Text style={styles.sectionLabel}>FORMULA BREAKDOWN</Text>
          <StatRow label="xA Gap  (×0.35)"     value={gap}                             highlight={player.xa_gap > 0} />
          <StatRow label="CC/Game (×0.30)"      value={player.chances_per_game.toFixed(2)} />
          <StatRow label="Big Chances (×0.20)"  value={player.big_chances} />
          <StatRow label="Penalties Won (×0.15)"value={player.penalties_won} />

          <View style={styles.divider} />

          <Text style={styles.sectionLabel}>RAW STATS</Text>
          <StatRow label="Assists"  value={player.assists} />
          <StatRow label="xA"       value={player.xa.toFixed(2)} />
          <StatRow label="xA Gap"   value={gap} highlight={player.xa_gap > 0} />
          {player.form_score != null && (
            <StatRow label="Form (last 5)" value={`${(player.form_score * 100).toFixed(0)}%`} />
          )}

          <TouchableOpacity style={styles.closeBtn} onPress={onClose}>
            <Text style={styles.closeBtnText}>Close</Text>
          </TouchableOpacity>
        </View>
      </View>
    </Modal>
  );
};

// ── Player Row ────────────────────────────────────────────────────────────────
const PlayerRow = ({ player, rank, onPress }) => (
  <TouchableOpacity style={styles.playerRow} onPress={() => onPress(player)}>
    <Text style={styles.rank}>{rank}</Text>
    <View style={styles.playerInfo}>
      <View style={styles.playerNameRow}>
        <Text style={styles.playerName} numberOfLines={1}>{player.player}</Text>
        {player.weak_opp_def && <Text style={styles.weakIcon}>🛡️</Text>}
      </View>
      <View style={styles.playerMetaRow}>
        <Text style={styles.playerSub}>{player.team}  ·  {shortLeague(player.league)}</Text>
        {player.form && player.form.length > 0 && <FormPills form={player.form} />}
      </View>
    </View>
    <Text style={[styles.score, { color: scoreColor(player.score) }]}>
      {player.score.toFixed(2)}
    </Text>
  </TouchableOpacity>
);

// ── Fixture Card ──────────────────────────────────────────────────────────────
const FixtureCard = ({ match, onPress }) => {
  const isLive     = match.live;
  const isFinished = match.finished;
  return (
    <TouchableOpacity
      style={[styles.fixtureCard, isLive && styles.fixtureCardLive]}
      onPress={() => onPress && onPress(match)}
      activeOpacity={0.75}>
      {isLive && (
        <View style={styles.liveBadge}>
          <Text style={styles.liveBadgeText}>● LIVE {match.minute}</Text>
        </View>
      )}
      <View style={styles.fixtureTeams}>
        <Text style={styles.fixtureTeam} numberOfLines={1}>{match.home}</Text>
        <View style={styles.fixtureScoreBox}>
          {(isLive || isFinished) && match.score
            ? <Text style={styles.fixtureScore}>{match.score}</Text>
            : <Text style={styles.fixtureTime}>{formatKickoff(match.kickoff)}</Text>
          }
        </View>
        <Text style={[styles.fixtureTeam, styles.fixtureTeamRight]} numberOfLines={1}>
          {match.away}
        </Text>
      </View>
    </TouchableOpacity>
  );
};

// ── Match Screen (full screen) ───────────────────────────────────────────────
const MatchScreen = ({ match, apiUrl, top25, onClose }) => {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);

  const base = apiUrl.replace(/\/$/, '');

  useEffect(() => {
    const fetchMatch = async () => {
      setLoading(true);
      try {
        const params = new URLSearchParams({
          home_id:  match.home_id,
          away_id:  match.away_id,
          home:     match.home,
          away:     match.away,
          live:     match.live     ? 'true' : 'false',
          finished: match.finished ? 'true' : 'false',
        });
        const res  = await fetch(`${base}/match/${match.match_id}?${params}`);
        const json = await res.json();
        if (json.error) throw new Error(json.error);
        setData(json);
      } catch (e) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };
    fetchMatch();
  }, [match.match_id]);

  const isLive     = match.live;
  const isFinished = match.finished;

  const PlayerSquadRow = ({ p, side }) => {
    const gap = p.xa_gap >= 0 ? `+${p.xa_gap}` : `${p.xa_gap}`;
    return (
      <View style={[styles.squadRow, p.in_top25 && styles.squadRowHighlight]}>
        <View style={styles.squadPlayerInfo}>
          <View style={styles.squadNameRow}>
            {p.in_top25 && <Text style={styles.starBadge}>★ </Text>}
            <Text style={[styles.squadPlayerName,
              p.in_top25 && styles.squadPlayerNameHighlight]}
              numberOfLines={1}>
              {p.player}
            </Text>
            {p.weak_opp && <Text style={styles.weakIconSmall}> 🛡️</Text>}
          </View>
          <Text style={styles.squadPlayerSub}>
            {`xA Gap ${gap}  ·  CC/G ${p.chances_per_game?.toFixed(2)}  ·  BC ${p.big_chances}`}
          </Text>
        </View>
        <Text style={[styles.squadScore, { color: scoreColor(p.score) }]}>
          {p.score?.toFixed(2)}
        </Text>
      </View>
    );
  };

  const LineupSection = ({ title, players }) => {
    if (!players || players.length === 0) return null;
    return (
      <View style={styles.lineupSection}>
        <Text style={styles.sectionLabel}>{title}</Text>
        <View style={styles.lineupGrid}>
          {players.map((p, i) => (
            <View key={i} style={[styles.lineupPill,
              p.in_top25 && styles.lineupPillHighlight]}>
              <Text style={[styles.lineupName,
                p.in_top25 && styles.lineupNameHighlight]}
                numberOfLines={1}>
                {p.in_top25 ? `★ ${p.name}` : p.name}
              </Text>
            </View>
          ))}
        </View>
      </View>
    );
  };

  return (
    <Modal animationType="slide" onRequestClose={onClose}>
      <SafeAreaView style={styles.safe}>
        <StatusBar barStyle="light-content" backgroundColor={C.bg} />

        {/* Header */}
        <View style={styles.matchHeader}>
          <TouchableOpacity onPress={onClose} style={styles.backBtn}>
            <Text style={styles.backBtnText}>← Back</Text>
          </TouchableOpacity>
          {isLive && (
            <View style={styles.livePill}>
              <Text style={styles.livePillText}>● LIVE {match.minute}</Text>
            </View>
          )}
        </View>

        {/* Score bar */}
        <View style={styles.scoreBar}>
          <Text style={styles.scoreTeam} numberOfLines={2}>{match.home}</Text>
          <View style={styles.scoreCenter}>
            {(isLive || isFinished) && match.score
              ? <Text style={styles.scoreLarge}>{match.score}</Text>
              : <Text style={styles.scoreVs}>vs</Text>}
            <Text style={styles.scoreKickoff}>{formatKickoff(match.kickoff)}</Text>
          </View>
          <Text style={[styles.scoreTeam, styles.scoreTeamRight]} numberOfLines={2}>
            {match.away}
          </Text>
        </View>

        {/* Weak def banners */}
        {data && (
          <View style={styles.weakDefRow}>
            {data.away_weak_def && (
              <View style={styles.weakDefBanner}>
                <Text style={styles.weakDefText}>🛡️ {match.away} weak defence</Text>
              </View>
            )}
            {data.home_weak_def && (
              <View style={styles.weakDefBanner}>
                <Text style={styles.weakDefText}>🛡️ {match.home} weak defence</Text>
              </View>
            )}
          </View>
        )}

        {loading && (
          <View style={styles.center}>
            <ActivityIndicator size="large" color={C.accent} />
            <Text style={[styles.emptySub, { marginTop: 12 }]}>
              {isLive ? 'Loading live data...' : 'Loading squad data...'}
            </Text>
          </View>
        )}

        {error && (
          <View style={styles.center}>
            <Text style={styles.errorText}>⚠ {error}</Text>
          </View>
        )}

        {data && !loading && (
          <ScrollView contentContainerStyle={{ paddingBottom: 40 }}>

            {/* Live lineups if available */}
            {data.lineups && (
              <>
                <LineupSection
                  title={`${match.home.toUpperCase()} LINEUP`}
                  players={data.lineups.home}
                />
                <LineupSection
                  title={`${match.away.toUpperCase()} LINEUP`}
                  players={data.lineups.away}
                />
                <View style={styles.divider} />
              </>
            )}

            {/* Home squad players */}
            {data.home_players?.length > 0 && (
              <>
                <Text style={styles.squadHeader}>
                  {match.home.toUpperCase()}
                  {data.away_weak_def ? '  🛡️ Weak opp' : ''}
                </Text>
                {data.home_players.slice(0, 8).map((p, i) => (
                  <PlayerSquadRow key={i} p={p} side="home" />
                ))}
              </>
            )}

            {/* Away squad players */}
            {data.away_players?.length > 0 && (
              <>
                <Text style={[styles.squadHeader, { marginTop: 8 }]}>
                  {match.away.toUpperCase()}
                  {data.home_weak_def ? '  🛡️ Weak opp' : ''}
                </Text>
                {data.away_players.slice(0, 8).map((p, i) => (
                  <PlayerSquadRow key={i} p={p} side="away" />
                ))}
              </>
            )}

            {data.home_players?.length === 0 && data.away_players?.length === 0 && (
              <View style={styles.center}>
                <Text style={styles.emptyText}>No player data for these teams</Text>
                <Text style={styles.emptySub}>
                  These teams may not be in your tracked leagues
                </Text>
              </View>
            )}
          </ScrollView>
        )}
      </SafeAreaView>
    </Modal>
  );
};

// ── Fixtures Tab ──────────────────────────────────────────────────────────────
const FixturesTab = ({ fixtures, loading, onLoad, onMatchPress }) => {
  if (loading) return (
    <View style={styles.center}>
      <ActivityIndicator size="large" color={C.accent} />
    </View>
  );
  if (!fixtures || Object.keys(fixtures).length === 0) return (
    <View style={styles.center}>
      <Text style={styles.emptyIcon}>📅</Text>
      <Text style={styles.emptyText}>No fixtures loaded</Text>
      <TouchableOpacity style={styles.loadBtn} onPress={onLoad}>
        <Text style={styles.loadBtnText}>Load Fixtures</Text>
      </TouchableOpacity>
    </View>
  );

  // Flatten + sort all matches by kickoff, group live first
  const allMatches = [];
  Object.entries(fixtures).forEach(([league, matches]) => {
    matches.forEach(m => allMatches.push({ ...m, league }));
  });

  const live     = allMatches.filter(m => m.live);
  const upcoming = allMatches.filter(m => !m.live && !m.finished)
                             .sort((a,b) => (a.kickoff||'').localeCompare(b.kickoff||''));
  const finished = allMatches.filter(m => m.finished)
                             .sort((a,b) => (b.kickoff||'').localeCompare(a.kickoff||''));

  const sections = [];
  if (live.length > 0)     sections.push({ title: '🔴 LIVE', data: live });
  if (upcoming.length > 0) sections.push({ title: 'UPCOMING', data: upcoming });
  if (finished.length > 0) sections.push({ title: 'RESULTS', data: finished });

  return (
    <ScrollView style={styles.flex} contentContainerStyle={{ paddingBottom: 40 }}>
      {sections.map(({ title, data }) => (
        <View key={title}>
          <Text style={styles.fixturesSectionHeader}>{title}</Text>
          {data.map((match, i) => (
            <View key={match.match_id || i}>
              <Text style={styles.fixtureLeagueLabel}>{shortLeague(match.league)}</Text>
              <FixtureCard match={match} onPress={onMatchPress} />
            </View>
          ))}
        </View>
      ))}
    </ScrollView>
  );
};

// ── League Drill-Down ─────────────────────────────────────────────────────────
const LeagueView = ({ byLeague, onPlayerPress }) => {
  const [openLeague, setOpenLeague] = useState(null);
  const [openTeam,   setOpenTeam]   = useState(null);
  return (
    <ScrollView style={styles.flex} contentContainerStyle={{ paddingBottom: 40 }}>
      {Object.entries(byLeague).map(([league, teams]) => (
        <View key={league}>
          <TouchableOpacity
            style={styles.leagueHeader}
            onPress={() => setOpenLeague(openLeague === league ? null : league)}>
            <Text style={styles.leagueTitle}>{league}</Text>
            <Text style={styles.chevron}>{openLeague === league ? '▲' : '▼'}</Text>
          </TouchableOpacity>
          {openLeague === league && teams.map(({ team, players }) => (
            <View key={team}>
              <TouchableOpacity
                style={styles.teamHeader}
                onPress={() => setOpenTeam(openTeam === team ? null : team)}>
                <Text style={styles.teamTitle}>{team}</Text>
                <Text style={styles.teamCount}>
                  {players.length} players  {openTeam === team ? '▲' : '▼'}
                </Text>
              </TouchableOpacity>
              {openTeam === team && players.map((p, i) => (
                <PlayerRow key={p.player+i} player={p} rank={i+1} onPress={onPlayerPress} />
              ))}
            </View>
          ))}
        </View>
      ))}
    </ScrollView>
  );
};

// ── Main App ──────────────────────────────────────────────────────────────────
export default function App() {
  const [apiUrl,   setApiUrl]   = useState(DEFAULT_API_URL);
  const [tab,      setTab]      = useState('fixtures');
  const [data,     setData]     = useState(null);
  const [fixtures, setFixtures] = useState(null);
  const [loading,  setLoading]  = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error,    setError]    = useState(null);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [selectedMatch,  setSelectedMatch]  = useState(null);
  const [urlInput, setUrlInput] = useState(DEFAULT_API_URL);

  const base = apiUrl.replace(/\/$/, '');

  const loadData = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const res  = await fetch(`${base}/data`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (json.error) throw new Error(json.error);
      setData(json);
    } catch (e) { setError(e.message); }
    finally { setLoading(false); }
  }, [base]);

  const loadFixtures = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const controller = new AbortController();
      const timeout    = setTimeout(() => controller.abort(), 8000);
      const res  = await fetch(`${base}/fixtures`, { signal: controller.signal });
      clearTimeout(timeout);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      // If cache empty just show empty state
      if (json.error) { setFixtures({}); setLoading(false); return; }
      setFixtures(json.fixtures || {});
    } catch (e) {
      setFixtures({});
      if (e.name !== 'AbortError') setError('Fixtures unavailable — hit Refresh');
    }
    finally { setLoading(false); }
  }, [base]);

  const runRefresh = useCallback(async () => {
    setRefreshing(true); setError(null);
    try {
      const res  = await fetch(`${base}/refresh`, { method: 'POST' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (!json.success) throw new Error(json.error || 'Refresh failed');
      setData(json);
      // Also reload fixtures
      await loadFixtures();
    } catch (e) { setError(e.message); }
    finally { setRefreshing(false); }
  }, [base, loadFixtures]);

  // Auto-load fixtures on mount
  useEffect(() => { loadFixtures(); }, []);

  const hasData = data && data.top25?.length > 0;

  return (
    <SafeAreaView style={styles.safe}>
      <StatusBar barStyle="light-content" backgroundColor={C.bg} />

      {/* Header */}
      <View style={styles.header}>
        <View>
          <Text style={styles.headerTitle}>ASSIST TOOL</Text>
          {data?.last_updated && (
            <Text style={styles.headerSub}>Updated {data.last_updated}</Text>
          )}
        </View>
        <TouchableOpacity
          style={[styles.refreshBtn, refreshing && styles.refreshBtnActive]}
          onPress={runRefresh}
          disabled={refreshing}>
          {refreshing
            ? <ActivityIndicator size="small" color={C.bg} />
            : <Text style={styles.refreshBtnText}>⟳  Refresh</Text>}
        </TouchableOpacity>
      </View>

      {error && (
        <View style={styles.errorBar}>
          <Text style={styles.errorText}>⚠  {error}</Text>
        </View>
      )}

      {/* Tabs */}
      <View style={styles.tabs}>
        {[['fixtures','Fixtures'],['top25','Top 25'],['leagues','By League'],['settings','Settings']].map(([key, label]) => (
          <TouchableOpacity
            key={key}
            style={[styles.tab, tab === key && styles.tabActive]}
            onPress={() => {
              setTab(key);
              if (key === 'fixtures' && !fixtures) loadFixtures();
              if ((key === 'top25' || key === 'leagues') && !hasData) loadData();
            }}>
            <Text style={[styles.tabText, tab === key && styles.tabTextActive]}>
              {label}
            </Text>
          </TouchableOpacity>
        ))}
      </View>

      {/* Content */}
      {tab === 'fixtures' && (
        <FixturesTab
          fixtures={fixtures}
          loading={loading}
          onLoad={loadFixtures}
          onMatchPress={setSelectedMatch}
        />
      )}

      {tab === 'top25' && (
        loading
          ? <View style={styles.center}><ActivityIndicator size="large" color={C.accent} /></View>
          : !hasData
            ? <View style={styles.center}>
                <Text style={styles.emptyIcon}>📊</Text>
                <Text style={styles.emptyText}>No data yet</Text>
                <Text style={styles.emptySub}>Hit Refresh to load player rankings</Text>
                <TouchableOpacity style={styles.loadBtn} onPress={loadData}>
                  <Text style={styles.loadBtnText}>Load Cached Data</Text>
                </TouchableOpacity>
              </View>
            : <FlatList
                data={data.top25}
                keyExtractor={(p,i) => p.player+i}
                renderItem={({ item, index }) => (
                  <PlayerRow player={item} rank={index+1} onPress={setSelectedPlayer} />
                )}
                contentContainerStyle={{ paddingBottom: 40 }}
                ListHeaderComponent={
                  <View>
                    <Text style={styles.listHeader}>TOP {data.top25?.length} — CROSS LEAGUE</Text>
                    <View style={styles.legendRow}>
                      <Text style={styles.legendItem}>🛡️ Weak opp defence</Text>
                      <View style={styles.legendDots}>
                        <View style={[styles.formDot, { backgroundColor: C.green }]} />
                        <View style={[styles.formDot, { backgroundColor: C.orange }]} />
                        <View style={[styles.formDot, { backgroundColor: C.red }]} />
                        <Text style={styles.legendLabel}> W/D/L</Text>
                      </View>
                    </View>
                  </View>
                }
              />
      )}

      {tab === 'leagues' && (
        loading
          ? <View style={styles.center}><ActivityIndicator size="large" color={C.accent} /></View>
          : !hasData
            ? <View style={styles.center}>
                <Text style={styles.emptyText}>No data yet</Text>
                <TouchableOpacity style={styles.loadBtn} onPress={loadData}>
                  <Text style={styles.loadBtnText}>Load Cached Data</Text>
                </TouchableOpacity>
              </View>
            : <LeagueView byLeague={data.by_league || {}} onPlayerPress={setSelectedPlayer} />
      )}

      {tab === 'settings' && (
        <ScrollView style={styles.flex} contentContainerStyle={styles.settingsContent}>
          <Text style={styles.settingsLabel}>API URL</Text>
          <Text style={styles.settingsHint}>Paste your Railway URL here. No trailing slash.</Text>
          <TextInput
            style={styles.urlInput}
            value={urlInput}
            onChangeText={setUrlInput}
            placeholder="https://xxxx.up.railway.app"
            placeholderTextColor={C.muted}
            autoCapitalize="none"
            autoCorrect={false}
          />
          <TouchableOpacity
            style={styles.saveBtn}
            onPress={() => { setApiUrl(urlInput.trim()); setData(null); setFixtures(null); setError(null); }}>
            <Text style={styles.saveBtnText}>Save & Reconnect</Text>
          </TouchableOpacity>
          <View style={styles.divider} />
          <Text style={styles.settingsLabel}>FORMULA</Text>
          {[['xA Gap','×0.35'],['CC / Game','×0.30'],['Big Chances','×0.20'],['Penalties Won','×0.15']].map(([l,w]) => (
            <View key={l} style={styles.formulaRow}>
              <Text style={styles.formulaLabel}>{l}</Text>
              <Text style={styles.formulaWeight}>{w}</Text>
            </View>
          ))}
          <View style={styles.divider} />
          <Text style={styles.settingsLabel}>FLAGS</Text>
          <View style={styles.formulaRow}>
            <Text style={styles.formulaLabel}>🛡️ Weak defence threshold</Text>
            <Text style={styles.formulaWeight}>GA/G ≥ 1.3</Text>
          </View>
        </ScrollView>
      )}

      <PlayerModal player={selectedPlayer} onClose={() => setSelectedPlayer(null)} />
      {selectedMatch && (
        <MatchScreen
          match={selectedMatch}
          apiUrl={apiUrl}
          top25={data?.top25 || []}
          onClose={() => setSelectedMatch(null)}
        />
      )}
    </SafeAreaView>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────
const styles = StyleSheet.create({
  safe:              { flex: 1, backgroundColor: C.bg },
  flex:              { flex: 1 },
  header:            { flexDirection: 'row', justifyContent: 'space-between',
                       alignItems: 'center', paddingHorizontal: 16, paddingVertical: 12,
                       borderBottomWidth: 1, borderBottomColor: C.border },
  headerTitle:       { color: C.accent, fontSize: 18, fontWeight: '800', letterSpacing: 2 },
  headerSub:         { color: C.muted, fontSize: 11, marginTop: 2 },
  refreshBtn:        { backgroundColor: C.accent, paddingHorizontal: 16,
                       paddingVertical: 8, borderRadius: 8 },
  refreshBtnActive:  { backgroundColor: C.accentDim },
  refreshBtnText:    { color: C.bg, fontWeight: '700', fontSize: 14 },
  errorBar:          { backgroundColor: '#3D1515', padding: 10, paddingHorizontal: 16 },
  errorText:         { color: C.red, fontSize: 13 },
  tabs:              { flexDirection: 'row', borderBottomWidth: 1, borderBottomColor: C.border },
  tab:               { flex: 1, paddingVertical: 10, alignItems: 'center' },
  tabActive:         { borderBottomWidth: 2, borderBottomColor: C.accent },
  tabText:           { color: C.muted, fontSize: 12, fontWeight: '600' },
  tabTextActive:     { color: C.accent },

  // Fixtures
  fixturesSectionHeader: { color: C.muted, fontSize: 11, fontWeight: '700',
                           letterSpacing: 1.5, paddingHorizontal: 16,
                           paddingTop: 16, paddingBottom: 6 },
  fixtureLeagueLabel:{ color: C.accentDim, fontSize: 10, fontWeight: '700',
                       letterSpacing: 1, paddingHorizontal: 16,
                       paddingTop: 8, paddingBottom: 2 },
  fixtureCard:       { marginHorizontal: 12, marginBottom: 4, backgroundColor: C.card,
                       borderRadius: 10, padding: 12,
                       borderWidth: 1, borderColor: C.border },
  fixtureCardLive:   { borderColor: C.red },
  liveBadge:         { marginBottom: 6 },
  liveBadgeText:     { color: C.red, fontSize: 11, fontWeight: '800', letterSpacing: 1 },
  fixtureTeams:      { flexDirection: 'row', alignItems: 'center' },
  fixtureTeam:       { flex: 1, color: C.text, fontSize: 13, fontWeight: '600' },
  fixtureTeamRight:  { textAlign: 'right' },
  fixtureScoreBox:   { paddingHorizontal: 10, minWidth: 80, alignItems: 'center' },
  fixtureScore:      { color: C.accent, fontSize: 16, fontWeight: '800' },
  fixtureTime:       { color: C.muted, fontSize: 11, textAlign: 'center' },

  // Player list
  listHeader:        { color: C.muted, fontSize: 11, fontWeight: '700',
                       letterSpacing: 1.5, padding: 16, paddingBottom: 4 },
  legendRow:         { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between',
                       paddingHorizontal: 16, paddingBottom: 8 },
  legendItem:        { color: C.muted, fontSize: 11 },
  legendDots:        { flexDirection: 'row', alignItems: 'center' },
  legendLabel:       { color: C.muted, fontSize: 11 },
  playerRow:         { flexDirection: 'row', alignItems: 'center',
                       paddingHorizontal: 16, paddingVertical: 10,
                       borderBottomWidth: 1, borderBottomColor: C.border },
  rank:              { color: C.muted, fontSize: 13, width: 28, fontWeight: '600' },
  playerInfo:        { flex: 1 },
  playerNameRow:     { flexDirection: 'row', alignItems: 'center' },
  playerName:        { color: C.text, fontSize: 15, fontWeight: '600', flex: 1 },
  weakIcon:          { fontSize: 13, marginLeft: 4 },
  playerMetaRow:     { flexDirection: 'row', alignItems: 'center',
                       marginTop: 3, gap: 8 },
  playerSub:         { color: C.muted, fontSize: 12 },
  score:             { fontSize: 17, fontWeight: '800', minWidth: 44, textAlign: 'right' },
  formRow:           { flexDirection: 'row', gap: 3 },
  formDot:           { width: 8, height: 8, borderRadius: 4 },

  // Empty
  center:            { flex: 1, alignItems: 'center', justifyContent: 'center', padding: 32 },
  emptyIcon:         { fontSize: 48, marginBottom: 12 },
  emptyText:         { color: C.text, fontSize: 18, fontWeight: '700' },
  emptySub:          { color: C.muted, fontSize: 14, marginTop: 6, textAlign: 'center' },
  loadBtn:           { marginTop: 24, borderWidth: 1, borderColor: C.accent,
                       paddingHorizontal: 24, paddingVertical: 10, borderRadius: 8 },
  loadBtnText:       { color: C.accent, fontWeight: '700' },

  // League
  leagueHeader:      { flexDirection: 'row', justifyContent: 'space-between',
                       alignItems: 'center', padding: 16, backgroundColor: C.surface,
                       borderBottomWidth: 1, borderBottomColor: C.border },
  leagueTitle:       { color: C.accent, fontSize: 14, fontWeight: '800', letterSpacing: 1 },
  chevron:           { color: C.muted, fontSize: 12 },
  teamHeader:        { flexDirection: 'row', justifyContent: 'space-between',
                       alignItems: 'center', paddingHorizontal: 24, paddingVertical: 10,
                       backgroundColor: C.card, borderBottomWidth: 1, borderBottomColor: C.border },
  teamTitle:         { color: C.text, fontSize: 13, fontWeight: '700' },
  teamCount:         { color: C.muted, fontSize: 12 },

  // Modal
  modalOverlay:      { flex: 1, backgroundColor: 'rgba(0,0,0,0.75)', justifyContent: 'flex-end' },
  modalCard:         { backgroundColor: C.surface, borderTopLeftRadius: 20,
                       borderTopRightRadius: 20, padding: 24, paddingBottom: 36 },
  modalHeader:       { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-start' },
  modalName:         { color: C.text, fontSize: 20, fontWeight: '800' },
  modalSub:          { color: C.muted, fontSize: 13, marginTop: 4 },
  scoreBadgeLarge:   { backgroundColor: C.card, borderRadius: 12,
                       paddingHorizontal: 14, paddingVertical: 8 },
  scoreBadgeText:    { fontSize: 24, fontWeight: '900' },
  nextOppRow:        { flexDirection: 'row', alignItems: 'center', flexWrap: 'wrap',
                       marginTop: 10, padding: 8, borderRadius: 8,
                       backgroundColor: C.card },
  nextOppRowWeak:    { backgroundColor: '#1A2510', borderWidth: 1, borderColor: C.green },
  nextOppLabel:      { color: C.muted, fontSize: 13 },
  nextOppTeam:       { color: C.text, fontSize: 13, fontWeight: '700' },
  weakBadge:         { color: C.green, fontSize: 13, fontWeight: '700' },
  nextOppTime:       { color: C.muted, fontSize: 12 },
  sectionLabel:      { color: C.muted, fontSize: 11, fontWeight: '700',
                       letterSpacing: 1.5, marginTop: 16, marginBottom: 8 },
  statRow:           { flexDirection: 'row', justifyContent: 'space-between',
                       paddingVertical: 7, borderBottomWidth: 1, borderBottomColor: C.border },
  statLabel:         { color: C.sub, fontSize: 14 },
  statValue:         { color: C.text, fontSize: 14, fontWeight: '600' },
  divider:           { height: 1, backgroundColor: C.border, marginVertical: 16 },
  closeBtn:          { marginTop: 20, backgroundColor: C.card, borderRadius: 10,
                       paddingVertical: 14, alignItems: 'center' },
  closeBtnText:      { color: C.text, fontWeight: '700', fontSize: 15 },

  // Settings
  settingsContent:   { padding: 20, paddingBottom: 60 },
  settingsLabel:     { color: C.muted, fontSize: 11, fontWeight: '700',
                       letterSpacing: 1.5, marginBottom: 8 },
  settingsHint:      { color: C.muted, fontSize: 13, marginBottom: 12, lineHeight: 18 },
  urlInput:          { backgroundColor: C.card, color: C.text, borderRadius: 10,
                       padding: 14, fontSize: 13, borderWidth: 1, borderColor: C.border,
                       marginBottom: 12 },
  saveBtn:           { backgroundColor: C.accent, borderRadius: 10,
                       paddingVertical: 14, alignItems: 'center' },
  saveBtnText:       { color: C.bg, fontWeight: '800', fontSize: 15 },
  formulaRow:        { flexDirection: 'row', justifyContent: 'space-between',
                       paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: C.border },
  formulaLabel:      { color: C.text, fontSize: 13, flex: 1 },
  formulaWeight:     { color: C.accent, fontSize: 14, fontWeight: '700' },

  // Match screen
  matchHeader:       { flexDirection: 'row', alignItems: 'center',
                       justifyContent: 'space-between', padding: 16,
                       borderBottomWidth: 1, borderBottomColor: C.border },
  backBtn:           { paddingVertical: 6, paddingRight: 16 },
  backBtnText:       { color: C.accent, fontSize: 15, fontWeight: '700' },
  livePill:          { backgroundColor: '#3D1515', borderRadius: 8,
                       paddingHorizontal: 10, paddingVertical: 4 },
  livePillText:      { color: C.red, fontSize: 12, fontWeight: '800', letterSpacing: 1 },
  scoreBar:          { flexDirection: 'row', alignItems: 'center',
                       paddingHorizontal: 16, paddingVertical: 20,
                       backgroundColor: C.surface },
  scoreTeam:         { flex: 1, color: C.text, fontSize: 15, fontWeight: '700' },
  scoreTeamRight:    { textAlign: 'right' },
  scoreCenter:       { alignItems: 'center', paddingHorizontal: 12, minWidth: 90 },
  scoreLarge:        { color: C.accent, fontSize: 28, fontWeight: '900' },
  scoreVs:           { color: C.muted, fontSize: 16, fontWeight: '600' },
  scoreKickoff:      { color: C.muted, fontSize: 11, marginTop: 4, textAlign: 'center' },
  weakDefRow:        { paddingHorizontal: 12, paddingVertical: 6, gap: 4 },
  weakDefBanner:     { backgroundColor: '#1A2510', borderRadius: 8, padding: 8,
                       borderWidth: 1, borderColor: C.green },
  weakDefText:       { color: C.green, fontSize: 13, fontWeight: '700' },
  squadHeader:       { color: C.muted, fontSize: 11, fontWeight: '700',
                       letterSpacing: 1.5, paddingHorizontal: 16,
                       paddingTop: 16, paddingBottom: 6 },
  squadRow:          { flexDirection: 'row', alignItems: 'center',
                       paddingHorizontal: 16, paddingVertical: 10,
                       borderBottomWidth: 1, borderBottomColor: C.border },
  squadRowHighlight: { backgroundColor: '#1C2415' },
  squadPlayerInfo:   { flex: 1 },
  squadNameRow:      { flexDirection: 'row', alignItems: 'center' },
  starBadge:         { color: C.accent, fontSize: 13, fontWeight: '800' },
  squadPlayerName:   { color: C.sub, fontSize: 14, fontWeight: '600', flex: 1 },
  squadPlayerNameHighlight: { color: C.text },
  weakIconSmall:     { fontSize: 12 },
  squadPlayerSub:    { color: C.muted, fontSize: 11, marginTop: 2 },
  squadScore:        { fontSize: 16, fontWeight: '800', minWidth: 40, textAlign: 'right' },
  lineupSection:     { paddingHorizontal: 16, paddingTop: 16 },
  lineupGrid:        { flexDirection: 'row', flexWrap: 'wrap', gap: 6, marginTop: 8 },
  lineupPill:        { backgroundColor: C.card, borderRadius: 16,
                       paddingHorizontal: 10, paddingVertical: 5,
                       borderWidth: 1, borderColor: C.border },
  lineupPillHighlight: { backgroundColor: '#1C2415', borderColor: C.accent },
  lineupName:        { color: C.muted, fontSize: 12 },
  lineupNameHighlight: { color: C.accent, fontWeight: '700' },
});
