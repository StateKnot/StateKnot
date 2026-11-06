# Alpha.1 historical-case production-input binding to final922

**160/160 recorded production input files are byte identical** between the executed 920-input historical-case reader (parent919 tree `bca1889b…`) and final922 candidate tree `88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb`. All actual 922 files and its canonical manifest tree were independently read and verified. The original test harness, after removing only the historical-case module declaration, is also entirely byte equal, so its reused reducer/schema validator/options helpers are unchanged.

The final922 candidate preserves every byte of previous921 and adds only the fixture provenance README. Against parent919 it changes two test modules and adds three test-fixture files; no production input differs. See `binding.json`, `candidate922-source-manifest.json`, the two production-input lists, `shared-test-harness-proof.json`, and `parent919-to-final922-test-only-changes.json`.

The original 5.3MB evidence archive, manifest, profile and seal were not modified. Its verifier including retained original binary/source checks passed again. The specific alpha.1 scenario was executed on the old 920-input experiment; this review compiled or executed nothing on final922. It proves file identity, **not identical test binaries, automatic qualification transfer, whole-source qualification or main adoption**. A maintainer must separately review any application of that exact historical-case evidence. No full compatibility, performance, capacity, full Native/CI or R1 claim follows.

中文：已独立核实最终922源码与冻结清单全部一致；与原 alpha.1 升级实验相同的 160 个生产输入逐字相等，复用父测试 harness（去掉新增 case 模块声明后）也完整相等。原证据包封签未动；本回执不将 920/922 说成同一源码或二进制，不自动转授具体 case 或全源码资格，主 schema35 未采用该候选。未编译、未运行数据库测试。
