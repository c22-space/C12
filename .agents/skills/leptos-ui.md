# Skill: Leptos UI Patterns
Leptos 0.7 (CSR/WASM) component and reactivity conventions for C12.

## When to use
When building or modifying any component in `ui/src/`.

## Instructions

### Signals
- Use `RwSignal<T>` with the most specific type — `RwSignal<Vec<EmissionSource>>`, never `RwSignal<Vec<serde_json::Value>>`
- Use `Memo::new(|_| ...)` for derived values — never recompute the same derivation inside multiple closures
- Use `ReadSignal` / `WriteSignal` pairs when you only need one direction

### Effects and async
- `Effect::new(|_| { ... })` for side effects that react to signal changes
- `spawn_local(async { ... })` only inside an `Effect` or a DOM event handler — never at the top level of a component
- Tauri commands are called via `spawn_local` wrapping an `async` block that calls `crate::tauri::<command>()`

### Input handling
- Use `on:blur` to save text field values — not `on:input`, which fires a Tauri call on every keystroke
- Use `on:change` for selects and checkboxes

### State sharing
- `provide_context(signal)` at a parent component, `use_context::<T>()` in descendants
- No prop-drilling past one level — lift state and use context instead

### Component structure
```rust
#[component]
pub fn MyComponent() -> impl IntoView {
    let items = use_context::<RwSignal<Vec<EmissionSource>>>().expect("context set at root");
    let total = Memo::new(move |_| items.get().iter().map(|s| s.emissions_tco2e).sum::<f64>());

    view! {
        <div>
            <p>{move || format!("{:.2} tCO₂e", total.get())}</p>
        </div>
    }
}
```

### What not to do
- Don't call `signal.get()` in a non-reactive context expecting it to update
- Don't use `create_signal` (Leptos 0.6 API) — use `RwSignal::new` or `signal()`
- Don't `.clone()` large vecs into every closure — use signals
