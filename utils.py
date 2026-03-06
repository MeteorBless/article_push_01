# -*- coding: utf-8 -*-
"""
【工具模块】
提供通用的辅助函数，如生成随机请求头、哈希计算、文本清洗等。
"""
import hashlib
import re
import random
from config import USER_AGENTS

def get_random_header():
    """从配置中随机获取一个 User-Agent"""
    return {"User-Agent": random.choice(USER_AGENTS)}

def sha1(s: str) -> str:
    """计算字符串的 SHA1 哈希值，用于生成指纹去重"""
    if not isinstance(s, str):
        s = str(s)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def clean_text(s: str) -> str:
    """
    通用文本清洗函数：
    1. 替换全角空格
    2. 去除多余的制表符、换行符
    3. 保留段落结构（双换行）
    """
    if not s:
        return ""
    s = s.replace("\u3000", " ")
    # 将连续的空白字符替换为单个空格
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    # 将3个以上的连续换行替换为2个换行（保留段落）
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()