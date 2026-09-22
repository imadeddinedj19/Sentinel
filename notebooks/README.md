# Notebooks

Exploratory work only — anything reusable belongs in the `sentinel` package.

Suggested order:

1. `01_data_exploration.ipynb` — what Binance actually returns, gaps and quirks.
2. `02_feature_analysis.ipynb` — distributions of the engineered features.
3. `03_detector_tuning.ipynb` — thresholds and validation against known events.

Notebooks assume the repo root is on the path:

```python
import sys; sys.path.append("..")
from sentinel import config
```
