# sqllogictest corpus: origin and version

These files are the SQLite **sqllogictest** test scripts, vendored gzip-compressed
(`gzip -9 -n`, contents byte-identical to upstream) and read directly by
`python -m nanosql.sqllogictest`.

* **Upstream:** SQLite's sqllogictest, <https://www.sqlite.org/sqllogictest/>
  (Fossil repository by D. Richard Hipp). The test scripts are public domain;
  the mirror's `COPYRIGHT.md` additionally offers GPL, BSD, MIT or CC0 at the
  user's choice ("No attribution is required").
* **Fetched from:** `www.sqlite.org` was not reachable from the session that
  vendored the corpus (egress proxy answered `403` to `CONNECT
  www.sqlite.org:443`), so the files were taken from the public GitHub mirror
  <https://github.com/gregrahn/sqllogictest>, commit
  `c93ac416f05635ff5cc490009add936d9753d01d` (2026-10-07, "Merge the 3.54.0
  beta sources for testing").
* **Selection:** all of `test/select1.test` … `test/select5.test`, plus a
  deterministic sample of the rest — the first file (`slt_good_0.test`) of
  every `test/index/<kind>/10/` and `test/random/<kind>/` directory, except
  `test/index/view/10/slt_good_0.test`, which is empty upstream (0 bytes), so
  `slt_good_1.test` is used — and four `test/evidence/` files.

| vendored file | upstream path | sha256 of the uncompressed file |
|---|---|---|
| `select1.test.gz` | `test/select1.test` | `e93b83d64d06f78aee0e690455b6c604e86ad9a339f77d927a782cefb6b0e1d5` |
| `select2.test.gz` | `test/select2.test` | `a8ecc3d206c4d4b2cd6a154c18999e558ec97168cd7e327a4369e23aaf31be64` |
| `select3.test.gz` | `test/select3.test` | `d5c321683bfe903c9be95ebe80a8d23da4d8f4a39193b2087dbc34cb4e137623` |
| `select4.test.gz` | `test/select4.test` | `155ff6bb9bbf7c2dcf1e5659bb1688dec5dab58126f8dc66d23dcae6df43f59e` |
| `select5.test.gz` | `test/select5.test` | `049a5d0bf90999c56db2d5880ef28febdc88906526f87069cc96af9f84c99869` |
| `evidence_in1.test.gz` | `test/evidence/in1.test` | `83d8958a4f86de196a0756e548f24c71db6edf2679d70393d546d542c84fc2fd` |
| `evidence_in2.test.gz` | `test/evidence/in2.test` | `2002fcdee665882916dea3de6172ba0ae9b5aae704d661d8f7daa810e1222383` |
| `evidence_slt_lang_aggfunc.test.gz` | `test/evidence/slt_lang_aggfunc.test` | `2a9d602e33c76766cba21d2f9a0e6ffda286f76445950ab187b544276c37abc9` |
| `evidence_slt_lang_update.test.gz` | `test/evidence/slt_lang_update.test` | `75c0dd5cc3ea56d33f584b7a8fdaa1fc362bb004deca9b0a519422decf5e2de2` |
| `index_between_10_slt_good_0.test.gz` | `test/index/between/10/slt_good_0.test` | `217a5476b90ab465d5a5b3a36772a45d1ba940d057fbee19434fd519155dd187` |
| `index_commute_10_slt_good_0.test.gz` | `test/index/commute/10/slt_good_0.test` | `6d2b95c95202c8c076f2ea75ab47c73d8922fee9e866ad901b27d3487b1c8d2c` |
| `index_delete_10_slt_good_0.test.gz` | `test/index/delete/10/slt_good_0.test` | `1cd78ec4325441b860708f1a91a4781493d8773c5150ca31a979e8914fd1bcee` |
| `index_in_10_slt_good_0.test.gz` | `test/index/in/10/slt_good_0.test` | `ddf09032471cc9e357c99f09224ee8c95f9318de4a61398c1e2bee013b6025d5` |
| `index_orderby_10_slt_good_0.test.gz` | `test/index/orderby/10/slt_good_0.test` | `803eaa3197d04e871be2b9be9763d3e1a64d2bfd4358852a5425b36dfe36165d` |
| `index_orderby_nosort_10_slt_good_0.test.gz` | `test/index/orderby_nosort/10/slt_good_0.test` | `83d641e793a34ecb09d2f38f971de62e8e6c5be44cd69a7761c6b1946416c50f` |
| `index_random_10_slt_good_0.test.gz` | `test/index/random/10/slt_good_0.test` | `7ea350ef65e31cf8ba1fcd652692ce766b2cf1f396d320e5bdd62d75ecbba19c` |
| `index_view_10_slt_good_1.test.gz` | `test/index/view/10/slt_good_1.test` | `86956600596ee5a15c8a6492dea5942d38e9b9b246c3fbe27f34037d2310acd9` |
| `random_aggregates_slt_good_0.test.gz` | `test/random/aggregates/slt_good_0.test` | `892136396d7de8e769775cacfef3c93a57f0bc961f0d7433ff5f1ba9050470f2` |
| `random_expr_slt_good_0.test.gz` | `test/random/expr/slt_good_0.test` | `90af1bebe0f8320c0221907bd7bcf1942c5bdf9ba9cf95642fe851136b9db42c` |
| `random_groupby_slt_good_0.test.gz` | `test/random/groupby/slt_good_0.test` | `e7ee14a796828b90c09aa79d8b258950b38af6936679d69a29fbd8ea8d9dd876` |
| `random_select_slt_good_0.test.gz` | `test/random/select/slt_good_0.test` | `ac3221c9a510007545b561ce2fe625b982c43df4d9d305d2e1d8d2f0048834c3` |
