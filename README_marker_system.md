# Marker System (Referent Analyzer)

## 1. What is a marker?
A marker is a lexical unit, phrase, discourse formula, or pattern that contributes to country image construction in a **referent-bound context**.

## 2. Dictionaries vs libraries
- **Dictionaries/rubrics** define scientific categories.
- **NLP libraries** are technical only (sentence split, tokenization, normalization/POS hints).

## 3. Role of NLP libraries
NLP is used only for preprocessing and content-word counting (`N_content`). Marker classification is dictionary/rule based.

## 4. IDI marker logic
IDI counts ideological markers and normalizes by content words:
`IDI = N_ideol / N_content`

## 5. EMI marker logic
EMI uses weighted emotional intensity:
`EMI = (1/3*N_e_w + 2/3*N_e_m + N_e_s) / N_content`

## 6. MTI marker logic
MTI counts contextually confirmed metaphor markers:
`MTI = N_met / N_content`

## 7. EVI rubric logic
`EVI = P - N`, `EVI_norm = EVI / 10`.
Scores come from direct evaluative lexis, attributed action, consequences, ideological frame, and discursive salience.

## 8. S_r salience logic
`S_r` values: `0`, `0.25`, `0.5`, `1`.
Technical mentions get `S_r=0` and are excluded from weighted aggregation.

## 9. Calibration and lexicon expansion
Candidate terms are extracted into `candidate_terms.csv`, then moved to `verified_terms.csv` or `rejected_terms.csv`.
All actions are logged in `dictionary_change_log.csv`.

## 10. Verification workflow
Every counted marker has a `MarkerTrace` row:
- source dictionary
- matched span
- inclusion/exclusion reason
- verification status

This makes every index and final IP auditable.
