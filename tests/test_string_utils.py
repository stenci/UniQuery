from string_utils import to_singular, to_plural, to_camel_case


def test_to_plural():
    assert to_plural("bus") == "buses"
    assert to_plural("city") == "cities"
    assert to_plural("fox") == "foxes"
    assert to_plural("woman") == "women"
    assert to_plural("child") == "children"
    assert to_plural("bus") == "buses"
    assert to_plural("city") == "cities"
    assert to_plural("fox") == "foxes"
    assert to_plural("ox") == "oxen"
    assert to_plural("child") == "children"
    assert to_plural("sky") == "skies"
    assert to_plural("box") == "boxes"
    assert to_plural("man") == "men"
    assert to_plural("wolf") == "wolves"
    assert to_plural("knife") == "knives"


def test_to_singular():
    assert to_singular("buses") == "bus"
    assert to_singular("cities") == "city"
    assert to_singular("foxes") == "fox"
    assert to_singular("oxen") == "ox"
    assert to_singular("children") == "child"
    assert to_singular("buses") == "bus"
    assert to_singular("cities") == "city"
    assert to_singular("foxes") == "fox"
    assert to_singular("women") == "woman"
    assert to_singular("children") == "child"
    assert to_singular("skies") == "sky"
    assert to_singular("boxes") == "box"
    assert to_singular("men") == "man"
    assert to_singular("wolves") == "wolf"
    assert to_singular("knives") == "knife"


def test_to_camel_case():
    assert to_camel_case('uno_due_tre') == 'UnoDueTre'
    assert to_camel_case('uno_dUE_tre') == 'UnoDueTre'
    assert to_camel_case('uno') == 'Uno'
    assert to_camel_case('UnoDueTre') == 'Unoduetre'
