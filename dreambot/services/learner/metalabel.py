"""Meta-labeling classifier wrapper."""
from __future__ import annotations

import numpy as np
from sklearn.linear_model import LogisticRegression


class MetaLabeler:
    def __init__(self):
        self.model = LogisticRegression(max_iter=500)
        self._fitted = False

    def fit(self, x: np.ndarray, y: np.ndarray) -> None:
        self.model.fit(x, y)
        self._fitted = True

    def predict_proba(self, x: np.ndarray) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("MetaLabeler must be fitted before use")
        return self.model.predict_proba(x)[:, 1]
