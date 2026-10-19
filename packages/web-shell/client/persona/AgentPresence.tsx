import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactElement,
} from 'react';
import type {
  AgentMood,
  AgentStatusLine,
  WebShellAgentPersona,
} from './agentPersona';
import styles from './AgentPresence.module.css';

/** The host's persona, or null (the default: no persona chrome at all). */
export const AgentPersonaContext = createContext<WebShellAgentPersona | null>(
  null,
);

export function useAgentPersona(): WebShellAgentPersona | null {
  return useContext(AgentPersonaContext);
}

const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(
    () =>
      typeof window !== 'undefined' &&
      Boolean(window.matchMedia?.(REDUCED_MOTION_QUERY).matches),
  );
  useEffect(() => {
    const query = window.matchMedia?.(REDUCED_MOTION_QUERY);
    if (!query) return;
    const update = () => setReduced(query.matches);
    query.addEventListener?.('change', update);
    return () => query.removeEventListener?.('change', update);
  }, []);
  return reduced;
}

/**
 * The agent's face in a round bubble. At rest (or under reduced motion) it
 * shows the mood's poster; with `animates` the loop plays over it, and a new
 * mood fades in over ~150 ms. Without an avatar URL it shows a monogram.
 *
 * The loop is WebM (VP9 with alpha). A browser that can't play it from this
 * server (WebKitGTK asks for byte ranges, which the gateway doesn't serve)
 * gets the same loop as animated WebP (`<mood>.webp`) instead.
 */
export function AgentAvatar({
  persona,
  mood = 'idle',
  size,
  animates = false,
  working = false,
}: {
  persona: WebShellAgentPersona;
  mood?: AgentMood;
  size: number;
  animates?: boolean;
  working?: boolean;
}): ReactElement {
  const reducedMotion = usePrefersReducedMotion();
  const [posterFailed, setPosterFailed] = useState(false);
  const [videoFailed, setVideoFailed] = useState(false);
  const base = persona.avatarBaseUrl;
  const showArt = Boolean(base) && !posterFailed;
  return (
    <span
      className={`${styles.avatar} ${working ? styles.avatarWorking : ''}`}
      style={{ width: size, height: size }}
      aria-hidden="true"
      data-agent-mood={mood}
    >
      {showArt ? (
        <>
          <img
            key={`poster-${mood}`}
            className={styles.avatarArt}
            src={`${base}/${mood}.png`}
            alt=""
            draggable={false}
            onError={() => setPosterFailed(true)}
          />
          {animates && !reducedMotion && videoFailed && (
            <img
              key={`loop-image-${mood}`}
              className={`${styles.avatarArt} ${styles.avatarLoop}`}
              src={`${base}/${mood}.webp`}
              alt=""
              draggable={false}
              data-agent-loop="webp"
            />
          )}
          {animates && !reducedMotion && !videoFailed && (
            <video
              key={`loop-${mood}`}
              className={`${styles.avatarArt} ${styles.avatarLoop}`}
              src={`${base}/${mood}.webm`}
              poster={`${base}/${mood}.png`}
              autoPlay
              muted
              loop
              playsInline
              disablePictureInPicture
              preload="auto"
              data-agent-loop="webm"
              onError={() => setVideoFailed(true)}
            />
          )}
        </>
      ) : (
        <span className={styles.monogram} style={{ fontSize: size * 0.45 }}>
          {Array.from(persona.name)[0]?.toUpperCase() ?? '•'}
        </span>
      )}
    </span>
  );
}

function formatElapsed(ms: number): string {
  const seconds = Math.max(0, Math.floor(ms / 1000));
  return `${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
}

function Elapsed({ startedAt }: { startedAt: number }): ReactElement {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, []);
  return (
    <span className={styles.elapsed}>{formatElapsed(now - startedAt)}</span>
  );
}

/**
 * The agent's face and status line at the live end of the transcript:
 * "Flame is reading the code… | App.tsx 0:14".
 */
export function AgentPresenceBubble({
  persona,
  line,
  subject,
  startedAt,
}: {
  persona: WebShellAgentPersona;
  line: AgentStatusLine;
  subject?: string;
  startedAt?: number;
}): ReactElement {
  const named = line.activity.kind !== 'done';
  return (
    <div
      className={styles.presenceRow}
      role="status"
      aria-live="polite"
      aria-label={line.sentence}
      data-testid="agent-presence"
    >
      <div className={styles.presence}>
        <AgentAvatar
          persona={persona}
          mood={line.mood}
          size={36}
          animates
          working={line.mood === 'working'}
        />
        <div className={styles.presencePill} aria-hidden="true">
          <span className={styles.presenceLine}>
            {named ? (
              <>
                <strong>{persona.name}</strong> {line.predicate}
              </>
            ) : (
              line.sentence
            )}
          </span>
          {subject && (
            <>
              <span className={styles.divider} />
              <span className={styles.subject}>{subject}</span>
            </>
          )}
          {startedAt !== undefined && <Elapsed startedAt={startedAt} />}
        </div>
      </div>
    </div>
  );
}

/** The small name label above an assistant reply. */
export function AgentReplyLabel({
  persona,
}: {
  persona: WebShellAgentPersona;
}): ReactElement {
  return (
    <div className={styles.replyLabel} data-testid="agent-reply-label">
      <AgentAvatar persona={persona} size={20} />
      <span className={styles.replyName}>{persona.name}</span>
    </div>
  );
}
