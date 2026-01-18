#!/usr/bin/env python3
"""
Data Agent v5.0 - 数据处理模块
处理第1章数据链
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path("/root/xiaoshuo")
STORAGE_PATH = PROJECT_ROOT / ".webnovel"
STATE_FILE = STORAGE_PATH / "state.json"
INDEX_DB = STORAGE_PATH / "index.db"
VECTORS_DB = STORAGE_PATH / "vectors.db"

# ========== Step A: 加载上下文 ==========
def load_context():
    """加载已有实体库和上下文"""
    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        state = json.load(f)

    entities = state.get('entities_v3', {})
    alias_index = state.get('alias_index', {})

    # 构建别名到实体的映射
    alias_to_entities = {}
    for alias, mappings in alias_index.items():
        for m in mappings:
            entity_id = m['id']
            entity_type = m['type']
            if alias not in alias_to_entities:
                alias_to_entities[alias] = []
            alias_to_entities[alias].append({'id': entity_id, 'type': entity_type})

    return state, entities, alias_to_entities

# ========== Step B: AI 实体提取 ==========
def extract_entities(chapter_content, existing_entities, alias_to_entities):
    """
    基于章节内容提取实体信息
    返回: entities_appeared, entities_new, state_changes, relationships_new, uncertain
    """
    entities_appeared = []
    entities_new = []
    state_changes = []
    relationships_new = []
    uncertain = []

    # 已知实体识别
    known_roles = existing_entities.get('角色', {})
    known_locations = existing_entities.get('地点', {})

    # 已出场的实体ID
    appeared_ids = set()

    # 1. 识别核心角色 - 叶凡
    if '叶凡' in chapter_content:
        entities_appeared.append({
            'id': 'yefan',
            'type': '角色',
            'mentions': ['叶凡', '他'],
            'confidence': 0.95
        })
        appeared_ids.add('yefan')

    # 2. 识别核心角色 - 顾晚晴
    if '顾晚晴' in chapter_content:
        entities_appeared.append({
            'id': 'guwanqing',
            'type': '角色',
            'mentions': ['顾晚晴', '她', '顾女士'],
            'confidence': 0.95
        })
        appeared_ids.add('guwanqing')

    # 3. 新实体识别 - 中年妇女（房东中介）
    if '中年妇女' in chapter_content and '房东直租' in chapter_content:
        entities_new.append({
            'suggested_id': 'middle_woman',
            'name': '中年妇女',
            'type': '角色',
            'tier': '装饰',
            'desc': '街边举牌招租的中年妇女，态度敷衍'
        })

    # 4. 地点识别 - 静雅小区
    if '静雅小区' in chapter_content:
        entities_new.append({
            'suggested_id': 'jingya_community',
            'name': '静雅小区',
            'type': '地点',
            'tier': '重要',
            'desc': '老小区，干净整洁，有3号楼602室'
        })

    # 5. 地点识别 - 3号楼602室
    if '602室' in chapter_content or '3号楼' in chapter_content:
        entities_new.append({
            'suggested_id': '602_room',
            'name': '602室',
            'type': '地点',
            'tier': '重要',
            'desc': '顾晚晴的出租房次卧'
        })

    # 6. 状态变化 - 叶凡租房成功
    if '租房' in chapter_content and '加微信' in chapter_content:
        state_changes.append({
            'entity_id': 'yefan',
            'field': 'location',
            'old': '无处可去',
            'new': '静雅小区3号楼602室',
            'reason': '租房成功'
        })
        state_changes.append({
            'entity_id': 'yefan',
            'field': 'status',
            'old': '找工作',
            'new': '租房中',
            'reason': '成为租客'
        })

    # 7. 状态变化 - 顾晚晴的房产状态
    if '租出去' in chapter_content or '还房贷' in chapter_content:
        state_changes.append({
            'entity_id': 'guwanqing',
            'field': 'location',
            'old': '顾晚晴的房产',
            'new': '静雅小区3号楼602室(房产)',
            'reason': '房子出租给叶凡'
        })

    # 8. 关系建立 - 房东与租客
    relationships_new.append({
        'from': 'yefan',
        'to': 'guwanqing',
        'type': '房东-租客',
        'description': '叶凡租住顾晚晴的房子'
    })

    # 9. 不确定项
    uncertain.append({
        'mention': '那位先生',
        'context': '中年妇女对叶凡的称呼',
        'candidates': [{'type': '角色', 'id': 'yefan'}],
        'confidence': 0.85
    })

    return entities_appeared, entities_new, state_changes, relationships_new, uncertain

# ========== Step C: 实体消歧处理 ==========
def disambiguate(uncertain):
    """处理消歧逻辑"""
    adopted = []
    warnings = []

    for item in uncertain:
        confidence = item.get('confidence', 0)
        if confidence > 0.8:
            adopted.append({
                'mention': item['mention'],
                'adopted': item['candidates'][0]['id'],
                'confidence': confidence,
                'reason': '高置信度直接采用'
            })
        elif confidence > 0.5:
            adopted.append({
                'mention': item['mention'],
                'adopted': item['candidates'][0]['id'],
                'confidence': confidence,
                'reason': '中置信度采用但记录警告'
            })
            warnings.append(f"中置信度匹配: {item['mention']} → {item['candidates'][0]['id']} (confidence: {confidence})")
        else:
            warnings.append(f"需人工确认: {item['mention']}")

    return adopted, warnings

# ========== Step D: 写入存储 - 更新 state.json ==========
def update_state(state, entities_appeared, entities_new, state_changes, relationships_new):
    """更新 state.json 文件"""
    entities_v3 = state.get('entities_v3', {})
    alias_index = state.get('alias_index', {})

    # 添加新实体
    for new_entity in entities_new:
        entity_id = new_entity['suggested_id']
        entity_type = new_entity['type']
        name = new_entity['name']

        if entity_type not in entities_v3:
            entities_v3[entity_type] = {}

        entities_v3[entity_type][entity_id] = {
            'id': entity_id,
            'canonical_name': name,
            'aliases': [],
            'tier': new_entity.get('tier', '普通'),
            'desc': new_entity.get('desc', ''),
            'current': {
                'realm': '普通人',
                'location': '',
                'status': '',
                'last_chapter': 1
            },
            'history': [],
            'created_chapter': 1,
            'first_appearance': '正文/第0001章.md'
        }

        # 更新别名索引
        if name not in alias_index:
            alias_index[name] = []
        alias_index[name].append({'type': entity_type, 'id': entity_id})

    # 更新状态变化
    for change in state_changes:
        entity_id = change['entity_id']
        field = change['field']
        new_value = change['new']

        # 查找实体
        found = False
        for entity_type in entities_v3:
            if entity_id in entities_v3[entity_type]:
                entity = entities_v3[entity_type][entity_id]
                old_value = entity['current'].get(field, '')
                entity['current'][field] = new_value
                entity['current']['last_chapter'] = 1

                # 记录历史
                entity['history'].append({
                    'chapter': 1,
                    'field': field,
                    'old': old_value,
                    'new': new_value,
                    'reason': change.get('reason', '')
                })
                found = True
                break

    # 添加关系
    if 'relationships' not in state:
        state['relationships'] = []

    for rel in relationships_new:
        state['relationships'].append({
            'from': rel['from'],
            'to': rel['to'],
            'type': rel['type'],
            'description': rel['description'],
            'chapter': 1,
            'created_at': datetime.now().isoformat()
        })

    # 更新元数据
    state['entities_v3'] = entities_v3
    state['alias_index'] = alias_index
    state['metadata']['current_chapter'] = 1
    state['metadata']['updated_at'] = datetime.now().isoformat()

    # 写入文件
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

    return state

# ========== Step D续: 创建 index.db ==========
def create_index_db():
    """创建或更新 index.db"""
    conn = sqlite3.connect(str(INDEX_DB))
    cursor = conn.cursor()

    # 创建章节表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chapters (
            chapter INTEGER PRIMARY KEY,
            title TEXT,
            location TEXT,
            word_count INTEGER,
            characters TEXT,
            scenes TEXT,
            created_at TEXT
        )
    ''')

    # 创建实体出场表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS entity_appearances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chapter INTEGER,
            entity_id TEXT,
            entity_type TEXT,
            mentions TEXT,
            confidence REAL,
            FOREIGN KEY (chapter) REFERENCES chapters(chapter)
        )
    ''')

    # 创建场景表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scenes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chapter INTEGER,
            scene_index INTEGER,
            location TEXT,
            summary TEXT,
            characters TEXT,
            start_line INTEGER,
            end_line INTEGER
        )
    ''')

    conn.commit()
    conn.close()

def insert_chapter_data(chapter, title, location, word_count, entities_appeared, scenes):
    """插入章节数据"""
    conn = sqlite3.connect(str(INDEX_DB))
    cursor = conn.cursor()

    characters = json.dumps([e['id'] for e in entities_appeared])
    scenes_json = json.dumps(scenes)

    cursor.execute('''
        INSERT OR REPLACE INTO chapters (chapter, title, location, word_count, characters, scenes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (chapter, title, location, word_count, characters, scenes_json, datetime.now().isoformat()))

    # 插入实体出场记录
    for entity in entities_appeared:
        cursor.execute('''
            INSERT INTO entity_appearances (chapter, entity_id, entity_type, mentions, confidence)
            VALUES (?, ?, ?, ?, ?)
        ''', (chapter, entity['id'], entity['type'], json.dumps(entity['mentions']), entity['confidence']))

    # 插入场景数据
    for scene in scenes:
        cursor.execute('''
            INSERT INTO scenes (chapter, scene_index, location, summary, characters, start_line, end_line)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (chapter, scene['index'], scene['location'], scene['summary'],
              json.dumps(scene['characters']), scene['start_line'], scene['end_line']))

    conn.commit()
    conn.close()

# ========== Step E: 场景切片 ==========
def chunk_scenes(chapter_content):
    """
    根据章节内容切分场景
    """
    scenes = []

    # 场景1: 街头找工作失败
    scene1 = {
        'index': 1,
        'start_line': 1,
        'end_line': 50,
        'location': '街头/写字楼',
        'summary': '叶凡面试失败，漫无目的地走在街头，回忆家族破产的落魄处境。',
        'characters': ['yefan']
    }

    # 场景2: 偶遇房东中介
    scene2 = {
        'index': 2,
        'start_line': 51,
        'end_line': 110,
        'location': '街边',
        'summary': '叶凡遇到举牌的中年妇女租房，被嫌弃价格高后离开。',
        'characters': ['yefan', 'middle_woman']
    }

    # 场景3: 看房签约
    scene3 = {
        'index': 3,
        'start_line': 111,
        'end_line': 280,
        'location': '静雅小区3号楼602室',
        'summary': '叶凡联系顾晚晴看房，两人因规矩和房租问题产生摩擦，最终签约成功。叶凡调侃顾晚晴的手机壳。',
        'characters': ['yefan', 'guwanqing']
    }

    # 场景4: 离别与感慨
    scene4 = {
        'index': 4,
        'start_line': 281,
        'end_line': 328,
        'location': '静雅小区门口/街头',
        'summary': '叶凡离开小区，望着只剩六百块的账户，感慨万千。',
        'characters': ['yefan']
    }

    scenes.extend([scene1, scene2, scene3, scene4])
    return scenes

# ========== Step F: 向量嵌入 (跳过) ==========
def skip_vector_embedding():
    """由于没有 RAG 服务，跳过向量嵌入"""
    print("[SKIP] 向量嵌入服务不可用，跳过此步骤")
    return []

# ========== Step G: 风格样本评估 ==========
def evaluate_style_sample(review_score, chapter_content):
    """评估并提取风格样本"""
    if review_score >= 80:
        # 高分章节，提取风格样本候选
        samples = []

        # 提取高质量片段
        if '手机壳' in chapter_content:
            samples.append({
                'type': '互动',
                'content': '叶凡调侃顾晚晴粉色手机壳的片段',
                'chapter': 1,
                'score': review_score
            })

        return samples
    return []

# ========== Step H: 生成处理报告 ==========
def generate_report(chapter, entities_appeared, entities_new, state_changes,
                   relationships_new, scenes, uncertain, warnings, adopted):
    """生成最终处理报告"""
    report = {
        'chapter': chapter,
        'entities_appeared': len(entities_appeared),
        'entities_new': len(entities_new),
        'state_changes': len(state_changes),
        'relationships_new': len(relationships_new),
        'scenes_chunked': len(scenes),
        'uncertain_resolved': len(adopted),
        'warnings': warnings,
        'details': {
            'entities_appeared': entities_appeared,
            'entities_new': entities_new,
            'state_changes': state_changes,
            'relationships_new': relationships_new,
            'scenes': scenes,
            'uncertain': uncertain,
            'adopted': adopted
        },
        'processed_at': datetime.now().isoformat()
    }

    return report

# ========== 主处理流程 ==========
def main():
    print("=" * 50)
    print("Data Agent v5.0 - 第1章数据处理")
    print("=" * 50)

    # 读取章节正文
    chapter_file = PROJECT_ROOT / "正文/第0001章.md"
    with open(chapter_file, 'r', encoding='utf-8') as f:
        chapter_content = f.read()

    # Step A: 加载上下文
    print("\n[Step A] 加载上下文...")
    state, existing_entities, alias_to_entities = load_context()
    print(f"  - 已加载实体: {len(existing_entities.get('角色', {}))} 个角色")

    # Step B: AI 实体提取
    print("\n[Step B] AI 实体提取...")
    entities_appeared, entities_new, state_changes, relationships_new, uncertain = extract_entities(
        chapter_content, existing_entities, alias_to_entities
    )
    print(f"  - 出场实体: {len(entities_appeared)}")
    print(f"  - 新实体: {len(entities_new)}")
    print(f"  - 状态变化: {len(state_changes)}")
    print(f"  - 新关系: {len(relationships_new)}")
    print(f"  - 待消歧项: {len(uncertain)}")

    # Step C: 实体消歧
    print("\n[Step C] 实体消歧处理...")
    adopted, warnings = disambiguate(uncertain)
    print(f"  - 已采用: {len(adopted)}")
    print(f"  - 警告: {len(warnings)}")

    # Step D: 更新存储
    print("\n[Step D] 更新数据存储...")

    # 更新 state.json
    print("  - 更新 state.json...")
    update_state(state, entities_appeared, entities_new, state_changes, relationships_new)

    # 创建并更新 index.db
    print("  - 更新 index.db...")
    create_index_db()
    scenes = chunk_scenes(chapter_content)
    insert_chapter_data(1, "租房", "静雅小区", 4500, entities_appeared, scenes)

    # Step E: 场景切片已完成 (在 D 中)
    print(f"\n[Step E] 场景切片: {len(scenes)} 个场景")

    # Step F: 向量嵌入 (跳过)
    print("\n[Step F] 向量嵌入...")
    skip_vector_embedding()

    # Step G: 风格样本评估
    print("\n[Step G] 风格样本评估...")
    style_samples = evaluate_style_sample(80, chapter_content)
    print(f"  - 提取风格样本: {len(style_samples)} 个")

    # Step H: 生成报告
    print("\n[Step H] 生成处理报告...")
    report = generate_report(
        1, entities_appeared, entities_new, state_changes,
        relationships_new, scenes, uncertain, warnings, adopted
    )

    # 保存报告
    report_file = STORAGE_PATH / f"report_chapter_1.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  - 报告已保存: {report_file}")

    # 打印最终汇总
    print("\n" + "=" * 50)
    print("处理结果汇总")
    print("=" * 50)
    print(f"章节: 1")
    print(f"出场实体: {report['entities_appeared']}")
    print(f"新实体: {report['entities_new']}")
    print(f"状态变化: {report['state_changes']}")
    print(f"新关系: {report['relationships_new']}")
    print(f"场景切片: {report['scenes_chunked']}")
    print(f"警告: {len(warnings)}")
    print("=" * 50)

    return report

if __name__ == "__main__":
    main()
