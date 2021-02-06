package sql

import (
	"bytes"
	"unicode"
	"unicode/utf8"
)

// Slug ...
type Slug struct {
	replacements map[rune]string
}

// NewSlug initializes and returns a new slug generator
func NewSlug(m map[rune]string) (s *Slug) {
	s = &Slug{}
	s.SetReplacements(m)

	return s
}

// Slugify takes a string input and converts it to a slug, removing special characters,
// normalizing accented characters and replacing space characters with dashes
func (s *Slug) Slugify(input string) (slug string) {
	b := bytes.Buffer{}
	for i, w := 0, 0; i < len(input); i += w {
		var runeValue rune
		runeValue, w = utf8.DecodeRuneInString(input[i:])
		newVal, ok := s.replacements[unicode.ToLower(runeValue)]
		if ok {
			_, _ = b.WriteString(newVal)
			continue
		}
		if unicode.IsSpace(runeValue) {
			_ = b.WriteByte('-')
		}
	}

	return b.String()
}

// SetReplacements defines the replacement map, if not specified, it uses the
// default mapping defined in getDefaultMap
func (s *Slug) SetReplacements(m map[rune]string) {
	if m == nil {
		m = s.getDefaultMap()
	}
	s.replacements = m
}

// getDefaultMap returns the default rune/string mappings
func (s *Slug) getDefaultMap() (m map[rune]string) {
	return map[rune]string{
		'a': "a",
		'b': "b",
		'c': "c",
		'd': "d",
		'e': "e",
		'f': "f",
		'g': "g",
		'h': "h",
		'i': "i",
		'j': "j",
		'k': "k",
		'l': "l",
		'm': "m",
		'n': "n",
		'o': "o",
		'p': "p",
		'q': "q",
		'r': "r",
		's': "s",
		't': "t",
		'u': "u",
		'v': "v",
		'w': "w",
		'x': "x",
		'y': "y",
		'z': "z",
		'0': "0",
		'1': "1",
		'2': "2",
		'3': "3",
		'4': "4",
		'5': "5",
		'6': "6",
		'7': "7",
		'8': "8",
		'9': "9",
		'-': "-",

		'&': "and",
		'@': "at",
		'©': "c",
		'®': "r",
		'Æ': "ae",
		'ß': "ss",
		'à': "a",
		'á': "a",
		'â': "a",
		'ä': "ae",
		'å': "a",
		'æ': "ae",
		'ç': "c",
		'è': "e",
		'é': "e",
		'ê': "e",
		'ë': "e",
		'ì': "i",
		'í': "i",
		'î': "i",
		'ï': "i",
		'ò': "o",
		'ó': "o",
		'ô': "o",
		'õ': "o",
		'ö': "oe",
		'ø': "o",
		'ù': "u",
		'ú': "u",
		'û': "u",
		'ü': "ue",
		'ý': "y",
		'þ': "p",
		'ÿ': "y",
		'ā': "a",
		'ă': "a",
		'Ą': "a",
		'ą': "a",
		'ć': "c",
		'ĉ': "c",
		'ċ': "c",
		'č': "c",
		'ď': "d",
		'đ': "d",
		'ē': "e",
		'ĕ': "e",
		'ė': "e",
		'ę': "e",
		'ě': "e",
		'ĝ': "g",
		'ğ': "g",
		'ġ': "g",
		'ģ': "g",
		'ĥ': "h",
		'ħ': "h",
		'ĩ': "i",
		'ī': "i",
		'ĭ': "i",
		'į': "i",
		'ı': "i",
		'ĳ': "ij",
		'ĵ': "j",
		'ķ': "k",
		'ĸ': "k",
		'Ĺ': "l",
		'ĺ': "l",
		'ļ': "l",
		'ľ': "l",
		'ŀ': "l",
		'ł': "l",
		'ń': "n",
		'ņ': "n",
		'ň': "n",
		'ŉ': "n",
		'ŋ': "n",
		'ō': "o",
		'ŏ': "o",
		'ő': "o",
		'Œ': "oe",
		'œ': "oe",
		'ŕ': "r",
		'ŗ': "r",
		'ř': "r",
		'ś': "s",
		'ŝ': "s",
		'ş': "s",
		'š': "s",
		'ţ': "t",
		'ť': "t",
		'ŧ': "t",
		'ũ': "u",
		'ū': "u",
		'ŭ': "u",
		'ů': "u",
		'ű': "u",
		'ų': "u",
		'ŵ': "w",
		'ŷ': "y",
		'ź': "z",
		'ż': "z",
		'ž': "z",
		'ſ': "z",
		'Ə': "e",
		'ƒ': "f",
		'Ơ': "o",
		'ơ': "o",
		'Ư': "u",
		'ư': "u",
		'ǎ': "a",
		'ǐ': "i",
		'ǒ': "o",
		'ǔ': "u",
		'ǖ': "u",
		'ǘ': "u",
		'ǚ': "u",
		'ǜ': "u",
		'ǻ': "a",
		'Ǽ': "ae",
		'ǽ': "ae",
		'Ǿ': "o",
		'ǿ': "o",
		'ə': "e",
		'Є': "e",
		'Б': "b",
		'Г': "g",
		'Д': "d",
		'Ж': "zh",
		'З': "z",
		'У': "u",
		'Ф': "f",
		'Х': "h",
		'Ц': "c",
		'Ч': "ch",
		'Ш': "sh",
		'Щ': "sch",
		'Ъ': "-",
		'Ы': "y",
		'Ь': "-",
		'Э': "je",
		'Ю': "ju",
		'Я': "ja",
		'а': "a",
		'б': "b",
		'в': "v",
		'г': "g",
		'д': "d",
		'е': "e",
		'ж': "zh",
		'з': "z",
		'и': "i",
		'й': "j",
		'к': "k",
		'л': "l",
		'м': "m",
		'н': "n",
		'о': "o",
		'п': "p",
		'р': "r",
		'с': "s",
		'т': "t",
		'у': "u",
		'ф': "f",
		'х': "h",
		'ц': "c",
		'ч': "ch",
		'ш': "sh",
		'щ': "sch",
		'ъ': "-",
		'ы': "y",
		'ь': "-",
		'э': "je",
		'ю': "ju",
		'я': "ja",
		'ё': "jo",
		'є': "e",
		'і': "i",
		'ї': "i",
		'Ґ': "g",
		'ґ': "g",
		'א': "a",
		'ב': "b",
		'ג': "g",
		'ד': "d",
		'ה': "h",
		'ו': "v",
		'ז': "z",
		'ח': "h",
		'ט': "t",
		'י': "i",
		'ך': "k",
		'כ': "k",
		'ל': "l",
		'ם': "m",
		'מ': "m",
		'ן': "n",
		'נ': "n",
		'ס': "s",
		'ע': "e",
		'ף': "p",
		'פ': "p",
		'ץ': "C",
		'צ': "c",
		'ק': "q",
		'ר': "r",
		'ש': "w",
		'ת': "t",
		'™': "tm",
		'ả': "a",
		'ã': "a",
		'ạ': "a",

		'ắ': "a",
		'ằ': "a",
		'ẳ': "a",
		'ẵ': "a",
		'ặ': "a",

		'ấ': "a",
		'ầ': "a",
		'ẩ': "a",
		'ẫ': "a",
		'ậ': "a",

		'ẻ': "e",
		'ẽ': "e",
		'ẹ': "e",
		'ế': "e",
		'ề': "e",
		'ể': "e",
		'ễ': "e",
		'ệ': "e",

		'ỉ': "i",
		'ị': "i",

		'ỏ': "o",
		'ọ': "o",
		'ố': "o",
		'ồ': "o",
		'ổ': "o",
		'ỗ': "o",
		'ộ': "o",
		'ớ': "o",
		'ờ': "o",
		'ở': "o",
		'ỡ': "o",
		'ợ': "o",

		'ủ': "u",
		'ụ': "u",
		'ứ': "u",
		'ừ': "u",
		'ử': "u",
		'ữ': "u",
		'ự': "u",

		'ỳ': "y",
		'ỷ': "y",
		'ỹ': "y",
		'ỵ': "y",
	}
}
