"""Run: python process/test_rule.py  — real tweets from the labelled sample."""
from rule import relevant

KEEP = [
    "Polymarket X account withheld in India after legal demand.",
    "Delhi High Court Orders Takedown Of Fake Kotak Trading Accounts From Social Media.",
    "Indian regime imposes two-month VPN ban in Udhampur as digital suppression of Kashmiris continues",
    "India Blocks Award-Winning Independent Film",
    "Kerala High Court seeks info from Centre, State on blocking Facebook page of news portal NoCap",
    "The Central government has filed a counter-affidavit before the Supreme Court defending its decision to suspend mobile internet services in parts of central Delhi",
    "India's .IN registry under MeitY suspended it without prior notice or disclosed grounds.",
    "Narendra Modi government hides the scale of content removal orders from Parliament.",
    "“We have restricted your access to your content in India pursuant to a notice from the Government of India.”",
]
DROP = [
    "@marvelstanguy @PerryALPHA Account based in India. Blocked",
    "South Korea blocks Polymarket over illegal gambling concerns",
    "@elonmusk Starlink should be banned in India forever.",
    "@imov18rse Tiktok is banned in India. Even with VPN some can't use it",
    "Twitter interim grievance officer for India quits amid row over new IT rules",
    "Press freedom in India facing severe challenges",
    "We are having Devcon this year in India and my uni just banned OKX & Coinbase sites from their Wi-Fi.",
    "@ravish_journo They will get a court order to take down all of Musk’s tweets or even worse block his account in India.",
]
for t in KEEP: assert relevant(t), t
for t in DROP: assert not relevant(t), t
print("ok", len(KEEP) + len(DROP))
