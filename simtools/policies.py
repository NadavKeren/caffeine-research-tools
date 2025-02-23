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
    pipeline = 'latency-aware.Pipeline'
    full_ghost = 'latency-aware.FGHC'
    sampled_ghost = 'latency-aware.SampledHillClimber'
    random_climber = 'latency-aware.RHC'
    arc = 'adaptive.Arc'
    frd = 'irr.Frd'
    
    
    ca_arc = "adaptive.CA-Arc"
    hyperbolic = "sampled.Hyperbolic"
    gdwheel = "greedy-dual.GDWheel"
    yan_li = 'latency-aware.YanLi'
