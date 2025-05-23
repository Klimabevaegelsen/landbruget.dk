def beregn_udsatte_hoener(a_hoennike: float, d_procent: float) -> float:
    """
    Beregner antallet af udsatte høner (A) i slutningen af et holds produktionsperiode.

    Args:
        a_hoennike: Antallet af indsatte hønniker på holdet.
        d_procent: Dødelighedsprocenten på holdet (som en decimal, e.g., 0.05 for 5%).

    Returns:
        Antallet af udsatte høner.
    """
    a = a_hoennike - (d_procent * a_hoennike)
    return a