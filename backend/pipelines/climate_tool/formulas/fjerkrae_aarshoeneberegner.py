def beregn_aarshoener(i_i: float, u_i: float, d_i: float) -> float:
    """
    Beregner antallet af årshøner (H) på et givent hold for et givent år.

    Args:
        i_i: Antal indsatte hønniker på det i'te hold.
        u_i: Antal udsatte høner på det i'te hold.
        d_i: Dage i perioden t.o.m 31/12 på det i'te hold.

    Returns:
        Antallet af årshøner.
    """
    h = (i_i + u_i) * 0.5 * (d_i / 365)
    return h