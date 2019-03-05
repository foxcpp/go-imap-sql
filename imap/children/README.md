### Children extension for go-imap

[RFC 3348](https://tools.ietf.org/html/rfc3348) extension for [go-imap](https://github.com/emersion/go-imap)


### Server

```
s.EnableExtension(children.NewExtension())
```

Used backend should have `EnableChildrenExt()` method which should return true.

