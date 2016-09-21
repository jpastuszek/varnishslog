Bench results
-------------

Run against commit c132f6622afaf947b08bcf479b4e853b6d2896f2 using 13480 VSL records making up 211 access records and 86 sessions and 86 client access records so about 157 VSL records per output client access record (benches/varnish20160816-4093-1xh1jbx808a493d5e74216e5.vsl).
This was run on MacBook Air (13-inch, Mid 2013) and build with rustc 1.10.0 (cfcb716cf 2016-07-03).

```
test log_session_record_ncsa_json ... bench:  25,649,614 ns/iter (+/- 5,243,613)
test log_session_record_json      ... bench:  25,758,141 ns/iter (+/- 2,806,616)
test log_session_record_json_raw  ... bench:  18,288,110 ns/iter (+/- 2,517,433)
test session_state                ... bench:  13,554,056 ns/iter (+/- 2,580,428)
test record_state                 ... bench:  15,072,194 ns/iter (+/- 2,582,783)

test custom_buffer_from_file_1mib ... bench:     631,408 ns/iter (+/- 242,004)
test custom_buffer_from_file_303b ... bench:   2,906,365 ns/iter (+/- 485,045)
test default_buffer               ... bench:     548,684 ns/iter (+/- 71,095)
test default_buffer_from_file     ... bench:     564,886 ns/iter (+/- 228,960)
test default_buffer_no_prefetch   ... bench:   2,569,675 ns/iter (+/- 151,935,685)
```

* 3.3 K/s JSON records (indexed) or 53 K/s VSL records processed
* 4.7 K/s JSON records (raw) or 74 K/s VSL records processed
* 6.3 K/s session records (correlated access records)
* 14 K/s access records
* 23 M/s VSL records
