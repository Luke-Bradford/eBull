# safety-state-ui

The smallest, sharpest frontend skill. Operator-safety indicators must survive the loading cycle.

## What counts as a safety-state indicator

Anything whose disappearance could let the operator make a wrong-state decision:

- Kill switch banner
- "Trading disabled" / "live mode on" pills
- "Coverage tier downgraded" warnings
- "Risk limit breached" indicators
- "Stale data" markers on positions or quotes
- "Halt flag set" notices
- Any "the system is in a dangerous state" UI

If unsure, ask: *"If this disappears for 2 seconds during a retry, could the operator click something they shouldn't?"* If yes, it's a safety-state indicator.

## Fail-safe rendering rule

**Never derive a safety banner directly from a refetchable async value.**

The hooks-and-API layer clears `data` to `null` at the start of every refetch (see `async-data-loading.md`). A banner derived from `system?.kill_switch.active` will disappear during any retry. That's a safety-visibility regression.

Instead: cache the last *confirmed* snapshot in component state and OR it with the live value.

```tsx
// WRONG — disappears the moment system.data goes null on refetch
{system?.kill_switch?.active && <KillSwitchBanner reason={system.kill_switch.reason} />}

// RIGHT — cached snapshot survives the loading cycle
const [cached, setCached] = useState<KillSwitchSnapshot | null>(null);
useEffect(() => {
  const fresh = system?.kill_switch ?? config?.kill_switch ?? null;
  if (fresh !== null) setCached({ ...fresh, fresh: true });
}, [system, config]);

const live = system?.kill_switch ?? config?.kill_switch ?? null;
const displayed = live
  ? { ...live, fresh: true }
  : cached
    ? { ...cached, fresh: false }
    : null;

{displayed?.active && (
  <KillSwitchBanner
    reason={displayed.reason}
    stale={!displayed.fresh}
  />
)}
```

## Clear-on-positive rule

A safety banner only clears when a fresh successful response **explicitly** says it should. Errors, retries, and loading states **never** clear it.

Concretely: if either source has ever reported `active=true` and a fresh `active=false` has not yet replaced it, the banner stays up.

The cache is only updated when a non-null fresh value arrives. A null value (loading or error) leaves the cache untouched. A `false` from a real successful response overwrites it.

## Stale marker is required, not optional

When the displayed snapshot comes from cache rather than a live response, mark it visibly. The operator must know the underlying source is in flight or has errored — they need to know the indicator is real but unverified.

```tsx
{!displayed.fresh && (
  <span className="text-[10px] uppercase text-red-500">(stale — refreshing)</span>
)}
```

This is non-negotiable. A silent cache that looks live is worse than no cache at all.

## Read-only on display surfaces, mutate on admin surfaces only

A safety-state indicator and its toggle never live in the same component. Display surfaces (dashboard, instrument detail) are strictly read-only. Mutation (the kill switch toggle) lives on the admin page only.

This mirrors the settled `kill switch separate from config flags` decision in `docs/settled-decisions.md`. If you find yourself adding an `onToggle` prop to a component on a dashboard, stop — you're on the wrong page.

## Cold-start rule

On the very first render, before any source has resolved, there is no cached snapshot. The banner does not show. This is correct: there is no prior state to be fail-safe about. As soon as either source resolves once, the cache is populated and the fail-safe rules above kick in.

## Pre-push checklist for safety-state UI

For every safety-state indicator you add or touch:

- [ ] Banner is derived from a cached snapshot, not from raw async data
- [ ] Cache update fires only on non-null fresh values
- [ ] Cache is cleared only on a fresh `false`, never on error or loading
- [ ] Stale marker is visible when the cache is the source
- [ ] No `onToggle` / `onChange` / `onMutate` prop on a display surface
- [ ] Tested mentally: hit Retry on the source endpoint — does the banner stay up?
