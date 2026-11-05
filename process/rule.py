"""API-free tier-2 relevance rule for the ambiguous embedding band.

Keep a tweet iff it names a blocking/takedown ACTION, has explicit Indian
state context, and is not a hypothetical / personal / foreign mention.

Tuned on ~330 hand-labelled tweets from the 0.40-0.58 band (Nov 2026).
Measured on fresh samples: ~45% of kept tweets relevant, ~80-90% of
relevant tweets kept (vs ~20% / 100% for keep-everything). Biased toward
recall on purpose: a missed takedown costs more than a reviewed false keep.
"""
import re

ACTION = re.compile(
    r"withh[eo]ld|\bblock(s|ed|ing)?\b|take ?down|taken down|\bban(s|ned)?\b|"
    r"restrict(s|ed|ion)|\bremov(e|ed|al)\b|\brestor(e|ed)\b|shut ?down|"
    r"\bsuspen(d|ds|ded|sion)\b|internet (services )?(barred|cut)|geo-?block|disabled|"
    r"legal demand|69a|blocking order", re.I)
INDIA = re.compile(
    r"\bindia|meity|\bmib\b|\bhc\b|high court|supreme court|it act|it rules|69a|"
    r"grievance appellate|\bgac\b|\bcentre\b|\bgovt\b|government|legal demand|"
    r"delhi|kashmir|kerala|telangana|tamil nadu|karnataka|madras|bombay|"
    r"modi\b|\bbjp\b|fiu-ind", re.I)
FOREIGN = re.compile(
    r"\b(china|chinese govt|south korea|korea|pakistan|bangladesh|russia|"
    r"australia|connecticut|ukraine|italy|brazil|nepal|\bus\b states?)\b", re.I)
NOISE = re.compile(
    # personal blocks / own-account complaints
    r"blocked (me|mine|him|her|them|you)|\bi blocked\b|immediately blocked|"
    r"account (from|based in) |\bindia(n)? account\b|my (\w+ )?account|"
    r"customer ?care|shadow[- ]?ban|my (uni|college|school|office)\b|wi-?fi|"
    # hypotheticals / wishes, not events
    r"\b(should|must|will|would|could|might|may|to) (be|get) (banned|blocked|withheld)|"
    r"get (you|him|x|it) (banned|blocked)|will get (a )?court order|"
    r"in ?case .{0,20}banned|request\w* .{0,30}withh|"
    # long-standing bans mentioned in passing
    r"(tiktok|it)('s| is| has been) banned (in india )?(for|since)|tiktok is banned", re.I)


def relevant(text: str) -> bool:
    return bool(ACTION.search(text) and INDIA.search(text)
                and not NOISE.search(text) and not FOREIGN.search(text))
