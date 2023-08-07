from enum import Enum

class Policy(Enum):
    opt =   'opt.Clairvoyant'
    lru =   'linked.Lru'
    lfu =   'linked.Lfu'
    wtlfu = 'sketch.WindowTinyLfu'
    hc =    'sketch.HillClimberWindowTinyLfu'
    naive_shadow = 'sketch.GhostClimberTinyLFU'
    window_ca = 'sketch.WindowCA'
    window_ca_burst_block = 'sketch.WindowCABurstBlock'
    window_ca_burst_cal = 'sketch.WindowCABurstCal'
    adaptive_ca = 'sketch.ACA'
    adaptive_ca_burst = 'sketch.AdaptiveCAWithBB'
    adaptive_pipeline = 'sketch.AdaptivePipeline'
    
    ca_arc = "adaptive.CA-Arc"
    hyperbolic = "sampled.Hyperbolic"
