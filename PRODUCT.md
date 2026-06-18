# Product

## Register

product

## Users

The primary user is the local owner of this API-key router, working on their own machine while testing clients, debugging provider availability, and checking which upstream handled each request.

## Product Purpose

This product provides a local admin surface for an OpenAI-compatible proxy that routes requests across multiple upstream API keys. Success means the user can quickly see service health, provider routing, recent usage, failures, and the exact upstream used by a request without relying on raw curl output.

## Brand Personality

Clear, practical, and calm. The interface should feel like a dependable local operations tool: dense enough for debugging, but not noisy or performative.

## Anti-references

Avoid marketing-style dashboards, decorative hero sections, oversized cards, fake analytics theatrics, or hiding important provider errors behind vague status labels.

## Design Principles

- Show the actual routing facts first.
- Keep local operations fast and legible.
- Prefer compact tables and status marks over explanatory prose.
- Make failure states easy to inspect without leaving the page.
- Preserve the project as a local-only personal tool.

## Accessibility & Inclusion

Target WCAG AA contrast. Support readable status labels in addition to color, keyboard-friendly controls, and reduced-motion behavior for any transitions.
