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
    sampled_ghost = 'latency-aware.SampledHillClimber'
    mock = 'latency-aware.Mockup'
    random_climber = 'latency-aware.RHC'
    arc = 'adaptive.Arc'
    frd = 'irr.Frd'
    s3_fifo = 'two-queue.S3Fifo'
    sieve = 'linked.Sieve'
    
    
    hyperbolic = "sampled.Hyperbolic"
    gdwheel = "greedy-dual.GDWheel"
    yan_li = 'latency-aware.YanLi'
