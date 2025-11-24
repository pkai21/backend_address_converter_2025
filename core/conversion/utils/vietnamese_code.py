import unicodedata, re

# ============================================
# BẢNG DỮ LIỆU CỐ ĐỊNH
# ============================================
# Bảng ánh xạ các nguyên âm tiếng Việt với 6 dạng thanh điệu:
# [không dấu, huyền, sắc, hỏi, ngã, nặng]
vowel_tone_map = {
    'a': ['a','à','á','ả','ã','ạ'],
    'ă': ['ă','ằ','ắ','ẳ','ẵ','ặ'],
    'â': ['â','ầ','ấ','ẩ','ẫ','ậ'],
    'e': ['e','è','é','ẻ','ẽ','ẹ'],
    'ê': ['ê','ề','ế','ể','ễ','ệ'],
    'i': ['i','ì','í','ỉ','ĩ','ị'],
    'o': ['o','ò','ó','ỏ','õ','ọ'],
    'ô': ['ô','ồ','ố','ổ','ỗ','ộ'],
    'ơ': ['ơ','ờ','ớ','ở','ỡ','ợ'],
    'u': ['u','ù','ú','ủ','ũ','ụ'],
    'ư': ['ư','ừ','ứ','ử','ữ','ự'],
    'y': ['y','ỳ','ý','ỷ','ỹ','ỵ']
}

# Tạo bảng tra ngược để tra nhanh:
# mỗi ký tự có dấu → (nguyên âm gốc, vị trí thanh điệu)
# ví dụ: "ắ" → ("ă", 2)
reverse_map = {
    ch: (base, idx)
    for base, forms in vowel_tone_map.items()
    for idx, ch in enumerate(forms)
}

# ============================================
# HÀM TRA CỨU
# ============================================
def get_base_and_tone(ch):
    """
    Chuẩn hóa ký tự (NFC), rồi tra xem:
      - Nó là nguyên âm nào (base)
      - Nó có thanh điệu mấy (0-5)
    Nếu không phải nguyên âm, trả về (None, 0)
    """
    ch_nfc = unicodedata.normalize('NFC', ch.lower())
    return reverse_map.get(ch_nfc, (None, 0))

# ============================================
# HÀM TÁCH TỪ THÀNH DANH SÁCH KÝ TỰ CÓ DẤU RIÊNG BIỆT
# ============================================
def decompose_word(word):
    """
    Chuyển từ sang dạng NFD (base + combining marks)
    rồi gom lại từng cụm ký tự thành từng nguyên âm đầy đủ.
    Ví dụ:
      "hoà" → ["h", "òa"]
    """
    nfd = unicodedata.normalize('NFD', word)
    chars, temp = [], ''
    for ch in nfd:
        # Nếu là ký tự gốc (non-combining)
        if unicodedata.combining(ch) == 0:
            if temp:
                chars.append(unicodedata.normalize('NFC', temp))
            temp = ch
        else:
            # Nếu là dấu kết hợp (combining accent)
            temp += ch
    if temp:
        chars.append(unicodedata.normalize('NFC', temp))
    return chars

# ============================================
# HÀM CHUẨN HÓA 1 ÂM TIẾT (VD: "hoà", "quyền")
# ============================================
def normalize_syllable(syll):
    """
    Nhận 1 âm tiết (từ đơn), chuẩn hóa lại vị trí dấu tiếng Việt.
    Không tự động thêm dấu nếu từ gốc không có.
    """
    if not syll:
        return syll

    chars = decompose_word(syll)
    vowel_positions, base_vowels = [], []
    tone_index = 0  # 0 = không dấu

    # Duyệt từng ký tự trong âm tiết
    for i, ch in enumerate(chars):
        base, t = get_base_and_tone(ch)
        if base:
            vowel_positions.append(i)
            base_vowels.append(base)
            if t != 0:
                tone_index = t  # lưu thanh điệu
            # tạm đặt ký tự này về dạng không dấu
            chars[i] = vowel_tone_map[base][0]

    # Nếu không có nguyên âm → trả lại như cũ
    if not vowel_positions:
        return syll

    # Nếu không có dấu thanh → không cần xử lý
    if tone_index == 0:
        return unicodedata.normalize('NFC', syll)

    # Bộ nguyên âm ưu tiên đặt dấu (như ê, ơ, â,...)
    modified_set = {'ê','ơ','â','ă','ô','ư'}

    # Chọn vị trí đặt dấu phù hợp
    pos = None
    for idx, base in zip(vowel_positions, base_vowels):
        if base in modified_set:
            pos = idx
            break

    # Nếu chưa có vị trí → xác định theo quy tắc tiếng Việt
    if pos is None:
        if len(vowel_positions) == 1:
            pos = vowel_positions[0]
        elif len(vowel_positions) == 2:
            # nếu nguyên âm đầu là "u" hoặc "i" → dấu vào nguyên âm sau
            if base_vowels[0] in {'u','i'}:
                pos = vowel_positions[1]
            else:
                pos = vowel_positions[0]
        else:
            # nếu có 3 nguyên âm (triphthong) → dấu ở giữa
            pos = vowel_positions[1]

    # Gắn lại dấu vào đúng nguyên âm
    target_base = base_vowels[vowel_positions.index(pos)]
    chars[pos] = vowel_tone_map[target_base][tone_index]

    # Ghép lại thành chuỗi
    result = ''.join(chars)

    # Giữ nguyên chữ hoa đầu nếu có
    if syll[0].isupper():
        result = result.capitalize()

    return result

# ============================================
# HÀM CHUẨN HÓA CẢ CÂU / VĂN BẢN
# ============================================
def vietnamese_normalize_text(text):
    """
    Chuẩn hóa toàn bộ câu, văn bản:
      - Tách ra từng từ, ký tự đặc biệt, khoảng trắng.
      - Chuẩn hóa từng âm tiết riêng biệt.
      - Giữ nguyên định dạng gốc (dấu câu, khoảng trắng, chữ hoa).
    """
    # Tách từ và ký tự không phải chữ (giữ nguyên thứ tự)
    words = re.findall(r'\w+|\W+', text, flags=re.UNICODE)
    normalized = []
    for token in words:
        if re.match(r'\w+', token, flags=re.UNICODE):
            # Nếu token là chữ (vd: "hoà bình")
            subwords = token.split()
            normalized.append(' '.join(normalize_syllable(sw) for sw in subwords))
        else:
            # Nếu là dấu câu, khoảng trắng thì giữ nguyên
            normalized.append(token)
    return ''.join(normalized)
