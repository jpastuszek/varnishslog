Bench results
-------------

Run against commit 5f46fc05dbc7f6ffd41b1b23c766629c33d22e9e using 13480 VSL records making up 211 access records and 86 sessions and 86 client access records so about 157 VSL records per output client access record (benches/varnish20160816-4093-1xh1jbx808a493d5e74216e5.vsl).
This was run on MacBook Air (13-inch, Mid 2013) and build with rustc 1.11.0 (9b21dcd6a 2016-08-15).

```
test log_session_record_ncsa_json ... bench:  16,968,762 ns/iter (+/- 4,440,508)
test log_session_record_json      ... bench:  16,930,045 ns/iter (+/- 4,514,501)
test log_session_record_json_raw  ... bench:  10,317,323 ns/iter (+/- 3,481,119)
test session_state                ... bench:   4,626,754 ns/iter (+/- 1,231,552)
test record_state                 ... bench:   5,974,753 ns/iter (+/- 1,232,293)

test custom_buffer_from_file_1mib ... bench:     631,408 ns/iter (+/- 242,004)
test custom_buffer_from_file_303b ... bench:   2,906,365 ns/iter (+/- 485,045)
test default_buffer               ... bench:     548,684 ns/iter (+/- 71,095)
test default_buffer_from_file     ... bench:     564,886 ns/iter (+/- 228,960)
test default_buffer_no_prefetch   ... bench:   2,569,675 ns/iter (+/- 151,935,685)
```

* 5.5 K/s JSON records (indexed) or 794 K/s VSL records processed into serialized output
* 8.3 K/s JSON records (raw) or 1306 K/s VSL records processed into serialized output
* 18.6 K/s session records (correlated access records) or 2913 K/s VSL records processed (without serialization)
* 14.4 K/s access records (not correlated into sessions) or 2256 K/s VSL records
* 24.6 M/s VSL records extracted from binary stream (message not parsed)
