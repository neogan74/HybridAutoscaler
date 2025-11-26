# HybridAutoscaler

## The Problem

VPA and HPA don't play well together out of the box:

- HPA scales horizontally based on CPU/memory utilization
- VPA adjusts resource requests/limits vertically
- When both target CPU/memory, they can fight each other and cause oscillations