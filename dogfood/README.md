# Dogfood graphs

Two runnable graphs proving Proceed models work beyond its own development loop.

- `delivery-loop.yaml` — lint/test (shell) → approve deploy (human approval) → deploy (HTTP).
- `content-publish.yaml` — draft (agent CLI) → editor review (human approval) → publish (HTTP).

```sh
proceed validate dogfood/delivery-loop.yaml
proceed validate dogfood/content-publish.yaml
```

The `127.0.0.1:1` URLs are placeholders: point them at a real target before
running. `content-publish.yaml` needs its `drafter` CLI registered in
`proceed.yaml` (`agent_clis`). Approval gates pause the run; grant with
`proceed approve`, then `proceed serve` continues it.
