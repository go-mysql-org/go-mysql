### Tag v1.1.1 (2021.04.08)
* Restructured dump/ tests. [#563](https://github.com/go-mysql-org/go-mysql/pull/563) ([atercattus](https://github.com/atercattus))
* Replace magic numbers in canal/canal_test.go by constants. [#562](https://github.com/go-mysql-org/go-mysql/pull/562) ([atercattus](https://github.com/atercattus))
* Fix parsing GTIDs from mysqlpdump. [#561](https://github.com/go-mysql-org/go-mysql/pull/561) ([dobegor](https://github.com/dobegor))
* Streaming of SELECT responses. `client/Conn.ExecuteSelectStreaming()` added. [#560](https://github.com/go-mysql-org/go-mysql/pull/560) ([atercattus](https://github.com/atercattus))
* Migation from travis.ci to github actions. [#559](https://github.com/go-mysql-org/go-mysql/pull/559) ([atercattus](https://github.com/atercattus))
* Output sorted mysql gtid. [#500](https://github.com/go-mysql-org/go-mysql/pull/500) ([zr-hebo](https://github.com/zr-hebo))
* Add skipped columns information. [#505](https://github.com/go-mysql-org/go-mysql/pull/505) ([laskoviymishka](https://github.com/laskoviymishka))
* Feat: support disable retry sync for canal. [#507](https://github.com/go-mysql-org/go-mysql/pull/507) ([everpcpc](https://github.com/everpcpc))
* Update README.md. [#511](https://github.com/go-mysql-org/go-mysql/pull/511) ([TennyZhuang](https://github.com/TennyZhuang))
* Add function to extend replication options. [#508](https://github.com/go-mysql-org/go-mysql/pull/508) ([wefen](https://github.com/wefen))

### Tag v1.1.0 (2020.07.17)
* Update .travis.yml (go 1.14 and tip). [#510](https://github.com/go-mysql-org/go-mysql/pull/510) ([atercattus](https://github.com/atercattus))
* Update README.md. [#509](https://github.com/go-mysql-org/go-mysql/pull/509) ([atercattus](https://github.com/atercattus))
* A lot of memory allocation optimizations. Changed public API for `mysql/Resultset` type. [#466](https://github.com/go-mysql-org/go-mysql/pull/466) ([atercattus](https://github.com/atercattus))

### Tag v1.0.0 (2020.07.17)
Add SemVer
