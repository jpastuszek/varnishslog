Bench results at c132f6622afaf947b08bcf479b4e853b6d2896f2 using 13480 VSL records making up 211 access records and 86 sessions and 86 client access records (benches/varnish20160816-4093-1xh1jbx808a493d5e74216e5.vsl):

```
test log_session_record_json      ... bench:  25,758,141 ns/iter (+/- 2,806,616)
test log_session_record_json_raw  ... bench:  18,288,110 ns/iter (+/- 2,517,433)
test log_session_record_ncsa_json ... bench:  25,649,614 ns/iter (+/- 5,243,613)
test record_state                 ... bench:  15,072,194 ns/iter (+/- 2,582,783)
test session_state                ... bench:  13,554,056 ns/iter (+/- 2,580,428)

test custom_buffer_from_file_1mib ... bench:     631,408 ns/iter (+/- 242,004)
test custom_buffer_from_file_303b ... bench:   2,906,365 ns/iter (+/- 485,045)
test default_buffer               ... bench:     548,684 ns/iter (+/- 71,095)
test default_buffer_from_file     ... bench:     564,886 ns/iter (+/- 228,960)
test default_buffer_no_prefetch   ... bench:   2,569,675 ns/iter (+/- 151,935,685)
```

So roughly:
* 3.3 K/s JSON records (indexed)
* 4.7 K/s JSON records (raw)
* 6.3 K/s session records
* 14 K/s access records
* 23 M/s VSL records
