//! The contract suite, run against the mock transport.
//!
//! This is the proof that the suite is transport-agnostic — and the template the
//! TCP transport will copy, one line long.

cs_testkit::transport_contract!(cs_testkit::MockFixture::new());
