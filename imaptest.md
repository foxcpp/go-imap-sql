# imaptest status

```
35 test groups: 9 failed, 0 skipped due to missing capabilities
base protocol: 10/366 individual commands failed
extensions: 16/26 individual commands failed
```

## Known issues

```
*** Test fetch-envelope command 1/2 (line 3)
 - failed: Missing 2 untagged replies (2 mismatches)
 - first unexpanded: 4 FETCH ($!unordered=2 ENVELOPE ("Thu, 15 Feb 2007 01:02:03 +0200" NIL (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) ((NIL NIL "group" NIL) (NIL NIL "g1" "d1.org") (NIL NIL "g2" "d2.org") (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL "g3" "d3.org") (NIL NIL NIL NIL)) ((NIL NIL "group" NIL) (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL NIL NIL)) NIL NIL NIL))
 - first expanded: 4 FETCH ( ENVELOPE ("Thu, 15 Feb 2007 01:02:03 +0200" NIL (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) ((NIL NIL "group" NIL) (NIL NIL "g1" "d1.org") (NIL NIL "g2" "d2.org") (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL "g3" "d3.org") (NIL NIL NIL NIL)) ((NIL NIL "group" NIL) (NIL NIL NIL NIL) (NIL NIL "group2" NIL) (NIL NIL NIL NIL)) NIL NIL NIL))
 - best match: 4 FETCH (ENVELOPE ("Thu, 15 Feb 2007 01:02:03 +0200" NIL (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) (("Real Name" NIL "user" "domain")) ((NIL NIL "g1" "d1.org") (NIL NIL "g2" "d2.org") (NIL NIL "g3" "d3.org")) NIL NIL NIL NIL))
 - Command: fetch 1:* envelope
```

No support for RFC 2822 group syntax in envelope parser.

```
*** Test search-addresses command 1/29 (line 3)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2 3 4 6 7
 - first expanded: search 1 2 3 4 6 7
 - best match: SEARCH 1 2 4 6 7
 - Command: search from user-from@domain.org 
```

No support for addresses with comments in search code.

```
*** Test search-size command 2/8 (line 9)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2
 - first expanded: search 1 2
 - best match: SEARCH 1 2 3 4
 - Command: search smaller $size

*** Test search-size command 3/8 (line 11)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 4
 - first expanded: search 4
 - best match: SEARCH
 - Command: search larger $size

*** Test search-size command 4/8 (line 13)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 3 4
 - first expanded: search 3 4
 - best match: SEARCH
 - Command: search not smaller $size

*** Test search-size command 5/8 (line 15)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2 3
 - first expanded: search 1 2 3
 - best match: SEARCH 1 2 3 4
 - Command: search not larger $size

*** Test search-size command 6/8 (line 18)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 3
 - first expanded: search 3
 - best match: SEARCH
 - Command: search not smaller $size not larger $size

*** Test search-size command 7/8 (line 20)
 - failed: Missing 1 untagged replies (1 mismatches)
 - first unexpanded: search 1 2 4
 - first expanded: search 1 2 4
 - best match: SEARCH 1 2 3 4
 - Command: search or smaller $size larger $size 
```

Size matcher fails to account for header fields size.