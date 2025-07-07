from typing import Optional

IRREGULAR_PLURALS = {
    'child': 'children',
    'ox':    'oxen',
    'man':   'men',
    'woman': 'women',
    'mouse': 'mice',
    'goose': 'geese',
}

IRREGULAR_SINGULARS = {v: k for k, v in IRREGULAR_PLURALS.items()}


def to_plural(word: str, custom_rules: Optional[dict] = None) -> str:
    """
    Convert a singular word to its plural form.

    :param word: The word to pluralize.
    :param custom_rules: A dictionary of custom pluralization rules.
    :return: The plural form of the word.
    """
    # If already plural, return as-is
    if is_plural(word, custom_rules):
        return word

    rules = {**IRREGULAR_PLURALS, **(custom_rules or {})}

    # Handle irregular cases
    if word in rules:
        return rules[word]

    # General rules
    if word.endswith('y') and not word.endswith(('ay', 'ey', 'iy', 'oy', 'uy')):
        return word[:-1] + 'ies'
    if word.endswith(('s', 'x', 'z', 'ch', 'sh')):
        return word + 'es'
    if word.endswith('an'):
        return word[:-2] + 'en'
    if word.endswith('lf'):
        return word[:-2] + 'lves'
    if word.endswith('fe'):
        return word[:-2] + 'ves'

    # Default rule
    return word + 's'


def to_singular(word: str, custom_rules: Optional[dict] = None) -> str:
    """
    Convert a plural word to its singular form.

    :param word: The word to singularize.
    :param custom_rules: A dictionary of custom singularization rules.
    :return: The singular form of the word.
    """
    rules = {**IRREGULAR_SINGULARS, **(custom_rules or {})}

    # Handle irregular cases
    if word in rules:
        return rules[word]

    # General rules
    if word.endswith('ies'):
        return word[:-3] + 'y'
    if word.endswith('es') and word[:-2].endswith(('s', 'x', 'z', 'ch', 'sh')):
        return word[:-2]
    if word.endswith('lves'):
        return word[:-3] + 'f'
    if word.endswith('ves'):
        return word[:-3] + 'fe'
    if word.endswith('s') and not word.endswith('ss'):
        return word[:-1]

    # Default rule
    return word


def to_camel_case(input_string: str) -> str:
    """
    Convert a snake_case string to CamelCase.

    :param input_string: The snake_case string to convert.
    :param capitalize_first: Whether to capitalize the first letter.
    :return: The CamelCase string.
    """
    words = input_string.split('_')
    return ''.join(word.capitalize() for word in words)


def is_plural(word: str, custom_rules: Optional[dict] = None) -> bool:
    """
    Determine if a word is plural.

    :param word: The word to check.
    :param custom_rules: A dictionary of custom pluralization rules.
    :return: True if the word is plural, False otherwise.
    """
    rules = {**IRREGULAR_SINGULARS, **(custom_rules or {})}

    # Check irregular cases
    if word in rules:
        return True

    # General checks
    if word.endswith('ies') or word.endswith('ves') or word.endswith('en'):
        return True
    if word.endswith('es') and not word[:-2].endswith(('s', 'x', 'z', 'ch', 'sh')):
        return False  # Avoid false positives for singular words like "bus"
    if word.endswith('s') and not word.endswith(('ss', 'us')):
        return True

    return False
