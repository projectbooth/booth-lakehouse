import jwt
import pytest

from booth_lakehouse_server.identity import AuthError, Forbidden, resolve, role_in_workspace

from .keys import CORE, IDP, Signer, verifier

idp, core = Signer(IDP), Signer(CORE)
v = verifier(idp, core)


def test_a_person_and_a_workload_are_both_accepted():
    assert v.verify(idp.token(sub="alice")).subject == "alice"
    # ADR 0056: notebook kernels and pipeline tasks call this module with core-minted run tokens.
    assert v.verify(core.token(sub="job:42")).subject == "job:42"


def test_untrusted_issuer_expired_and_forged_tokens_are_401():
    with pytest.raises(AuthError, match="not trusted"):
        v.verify(Signer("https://evil.example").token())
    with pytest.raises(AuthError):
        v.verify(idp.token(ttl=-10))
    with pytest.raises(AuthError):  # right issuer, wrong key
        v.verify(Signer(IDP).token())
    with pytest.raises(AuthError):  # HS256 signed with the public key: algorithm confusion
        v.verify(jwt.encode({"iss": IDP, "sub": "x", "exp": 9999999999}, "secret", algorithm="HS256"))
    with pytest.raises(AuthError, match="malformed"):
        v.verify("not-a-jwt")


def test_role_is_the_tokens_and_a_stronger_forwarded_role_is_rejected():
    claims = v.verify(idp.token(groups=["/workspaces/acme/viewer", "/workspaces/beta/owner"]))
    assert resolve(claims, "t", "acme").role == "viewer"
    with pytest.raises(Forbidden, match="exceeds"):
        resolve(claims, "t", "acme", forwarded_role="editor")  # ADR 0041
    assert resolve(claims, "t", "beta", forwarded_role="viewer").role == "viewer"  # narrowing is fine
    with pytest.raises(Forbidden, match="no role"):
        resolve(claims, "t", "gamma")
    with pytest.raises(Forbidden):
        resolve(claims, "t", "Not A Slug")


def test_group_grammar_is_strict_and_highest_role_wins():
    assert role_in_workspace(["/workspaces/acme/editor", "/workspaces/acme/owner"], "acme") == "owner"
    assert role_in_workspace(["/workspaces/acme/admin", "workspaces/acme/owner", "/workspaces/acme-x/owner"], "acme") == ""


def test_identity_repr_never_contains_the_token():
    who = resolve(v.verify(idp.token()), "SECRET-TOKEN", "acme")
    assert "SECRET-TOKEN" not in repr(who)
    with pytest.raises(Forbidden):
        who.require("owner")
