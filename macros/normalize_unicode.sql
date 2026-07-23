{% macro normalize_unicode(column) %}

    -- {#
    --     Approximates Python's normalize_fancy_unicode():
    --       1. translate() maps full-width Latin A-Z / a-z → ASCII
    --          (Unicode block U+FF01–U+FF5E, shifted by 0xFEE0)
    --       2. translate() maps common accented chars → their ASCII base
    --       3. lower() + trim() to finish

    --     Does NOT handle:
    --       - Fraktur / mathematical alphanumerics (rare in practice)
    --       - Combining diacritical marks that survived decomposition
    --       These would require a regex replace loop not available in
    --       standard SQL; add a seed-based lookup if they show up.
    -- #}

    lower(
        trim(
            translate(
                translate(
                    {{ column }},

                    -- ── Pass 1: full-width Latin → ASCII ──────────────────
                    -- Full-width uppercase A–Z  (U+FF21–U+FF3A)
                    -- Full-width lowercase a–z  (U+FF41–U+FF5A)
                    -- Full-width digits  0–9    (U+FF10–U+FF19)
                    'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ'
                    'ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ'
                    '０１２３４５６７８９',
                    'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                    'abcdefghijklmnopqrstuvwxyz'
                    '0123456789'
                ),

                -- ── Pass 2: accented / diacritic → ASCII base ─────────────
                -- Covers the characters most likely to appear in
                -- Indonesian names (borrowed Dutch/Portuguese spellings)
                -- and stray European chars from autocorrect.
                'ÀÁÂÃÄÅàáâãäåÈÉÊËèéêëÌÍÎÏìíîïÒÓÔÕÖØòóôõöøÙÚÛÜùúûüÝýÿÑñÇçŠšŽžŸ',
                'AAAAAAaaaaaaEEEEeeeeIIIIiiiiOOOOOOooooooUUUUuuuuYyyNnCcSsZzY'
            )
        )
    )

{% endmacro %}