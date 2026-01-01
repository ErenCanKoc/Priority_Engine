# src/playbook_library.py
"""
SEO Action Playbook Library (NICE TO HAVE #10)

Provides step-by-step playbooks for each action type.
Content team can follow these without SEO expertise.
"""

PLAYBOOKS = {
    "ctr_optimization_v1": {
        "name": "Click-Through Rate Optimization",
        "description": "Improve title and meta description to get more clicks from search results",
        "effort": "low",
        "estimated_time": "30-60 minutes",
        "owner": "content_team or seo_content",
        "steps": [
            {
                "step": 1,
                "title": "Analyze Current Title",
                "action": "Look at current page title. Does it include the main keyword? Is it compelling?",
                "checklist": [
                    "Keyword present in first 60 characters?",
                    "Benefit or value proposition clear?",
                    "Emotional trigger present? (kostenlos, einfach, schnell, etc.)",
                ]
            },
            {
                "step": 2,
                "title": "Research Top 3 Competitors",
                "action": "Google the keyword. What do position 1-3 titles say? What patterns do you see?",
                "checklist": [
                    "Common words/phrases across top results?",
                    "What benefits do they emphasize?",
                    "What's missing in our title?",
                ]
            },
            {
                "step": 3,
                "title": "Rewrite Title",
                "action": "Create new title following this formula: [Keyword] | [Benefit] | [Brand]",
                "example": "Formular Erstellen | Kostenlos & Einfach | Jotform",
                "checklist": [
                    "Under 60 characters (mobile display)?",
                    "Keyword in first 40 characters?",
                    "Clear benefit stated?",
                    "Action-oriented language?",
                ]
            },
            {
                "step": 4,
                "title": "Update Meta Description",
                "action": "Write compelling description (150-160 characters) that expands on the benefit",
                "example": "Erstellen Sie professionelle Formulare in 5 Minuten. Keine Anmeldung erforderlich. Über 100 Vorlagen kostenlos verfügbar.",
                "checklist": [
                    "Under 160 characters?",
                    "Includes keyword naturally?",
                    "Clear call to action?",
                    "Specific details (numbers, time, benefits)?",
                ]
            },
            {
                "step": 5,
                "title": "Test and Monitor",
                "action": "Implement changes and track CTR in Google Search Console after 2 weeks",
                "success_metric": "10-20% CTR improvement expected",
            }
        ],
        "common_mistakes": [
            "Keyword stuffing (repeating keyword multiple times)",
            "Generic phrases ('the best', 'high quality') without specifics",
            "Missing emotional triggers for German audience (kostenlos, sicher, einfach)",
            "Title too long (cut off in mobile results)",
        ],
        "examples": {
            "bad": "Online Form Builder - Create Forms | Company Name",
            "good": "Formular Erstellen | Kostenlos in 5 Min | 100+ Vorlagen",
        }
    },
    
    "snippet_capture_v1": {
        "name": "Featured Snippet Optimization",
        "description": "Optimize content to capture Google's answer box (position 0)",
        "effort": "medium",
        "estimated_time": "2-3 hours",
        "owner": "content_team",
        "steps": [
            {
                "step": 1,
                "title": "Identify Target Question",
                "action": "What question is the featured snippet answering? Search the keyword and read the current snippet.",
                "checklist": [
                    "What's the exact question format? (What is...? How to...?)",
                    "Is it a definition, list, or steps?",
                    "Who currently owns the snippet?",
                ]
            },
            {
                "step": 2,
                "title": "Add Definition Box",
                "action": "If it's a 'what is' question, add a clear definition at the top of your content",
                "format": "40-60 words, no fluff, direct answer",
                "example": "Ein Formular ist ein strukturiertes Dokument zum Sammeln von Informationen. Online-Formulare ermöglichen digitale Datenerfassung für Umfragen, Anmeldungen, Bestellungen und mehr.",
                "placement": "Right after H1, before main content",
            },
            {
                "step": 3,
                "title": "Use Structured Format",
                "action": "Format content for snippet-friendliness",
                "formats": {
                    "paragraph": "40-60 word definition in <p> tag",
                    "list": "<ul> or <ol> with 3-8 items, each 1-2 lines",
                    "table": "<table> for comparisons, 2-4 columns, 3-6 rows",
                    "steps": "Numbered list with clear action verbs",
                },
            },
            {
                "step": 4,
                "title": "Add FAQ Schema (Optional)",
                "action": "If you have FAQ section, use schema markup",
                "note": "Ask SEO team for help with schema implementation",
            },
            {
                "step": 5,
                "title": "Monitor Results",
                "action": "Check in 2-4 weeks if you captured the snippet",
                "success_metric": "Featured snippet capture = 30-40% CTR increase",
            }
        ],
        "common_mistakes": [
            "Definition too long (Google prefers 40-60 words)",
            "Too much marketing language (Google prefers factual)",
            "Not answering the exact question users ask",
            "List items too long (keep under 2 lines each)",
        ],
        "examples": {
            "bad": "Our amazing form builder is the best solution for creating professional forms...",
            "good": "Ein Formular-Builder ist ein Tool zum Erstellen digitaler Formulare ohne Code. Nutzer wählen Felder, passen Design an und teilen den Link.",
        }
    },
    
    "faq_expansion_v1": {
        "name": "FAQ Expansion for PAA Capture",
        "description": "Add FAQ section to capture 'People Also Ask' clicks",
        "effort": "low",
        "estimated_time": "1-2 hours",
        "owner": "content_team",
        "steps": [
            {
                "step": 1,
                "title": "Research PAA Questions",
                "action": "Google your keyword. Expand all 'People Also Ask' questions. Write them down.",
                "tip": "Use incognito mode to see unbiased results",
            },
            {
                "step": 2,
                "title": "Select Top 5-8 Questions",
                "action": "Choose most relevant questions for your page topic",
                "criteria": [
                    "Directly related to page topic",
                    "Answerable with your expertise",
                    "Different enough from each other",
                    "Common user concerns",
                ]
            },
            {
                "step": 3,
                "title": "Write Clear Answers",
                "action": "Answer each question in 2-4 sentences (50-100 words)",
                "format": {
                    "question": "Use exact wording from PAA",
                    "answer": "Direct answer first, then supporting details",
                    "length": "50-100 words per answer",
                },
                "example": {
                    "question": "Wie erstelle ich ein Formular online?",
                    "answer": "Wählen Sie eine Vorlage oder starten Sie von Grund auf. Fügen Sie Felder hinzu (Name, Email, etc.), passen Sie das Design an, und klicken Sie auf 'Veröffentlichen'. Der Link kann dann per Email oder auf Ihrer Website geteilt werden.",
                }
            },
            {
                "step": 4,
                "title": "Format FAQ Section",
                "action": "Add as dedicated section with clear heading",
                "placement": "After main content, before conclusion",
                "heading": "Häufig gestellte Fragen (FAQ)" or "Häufige Fragen",
                "format": "Use <h2> for section, <h3> for each question",
            },
            {
                "step": 5,
                "title": "Link FAQ in Table of Contents",
                "action": "If page has TOC, add FAQ section link",
            }
        ],
        "common_mistakes": [
            "Answers too long (keep under 100 words)",
            "Using different wording than PAA (use exact questions)",
            "Too many questions (5-8 is optimal)",
            "Promotional answers (be factual, helpful)",
        ],
        "impact": "10-15% click increase from capturing PAA queries",
    },
    
    "content_expansion_v1": {
        "name": "Strategic Content Expansion",
        "description": "Add new sections to increase topical coverage and rankings",
        "effort": "medium",
        "estimated_time": "3-5 hours",
        "owner": "content_team",
        "steps": [
            {
                "step": 1,
                "title": "Content Gap Analysis",
                "action": "Compare your content to top 3 ranking pages",
                "checklist": [
                    "What sections do they have that you don't?",
                    "What subtopics do they cover?",
                    "What examples/use cases do they mention?",
                    "What questions do they answer?",
                ]
            },
            {
                "step": 2,
                "title": "Identify Target Keywords",
                "action": "Find 3-5 related keywords to target with new sections",
                "tools": "Google 'related searches' at bottom of search results",
                "example": "Main: 'formular erstellen' → Related: 'anmeldeformular erstellen', 'kontaktformular erstellen', 'pdf formular erstellen'",
            },
            {
                "step": 3,
                "title": "Outline New Sections",
                "action": "Plan 2-3 new sections (300-500 words each)",
                "structure": [
                    "Section title with keyword (H2)",
                    "Brief intro paragraph",
                    "3-5 bullet points or numbered steps",
                    "Example or use case",
                    "Summary sentence",
                ]
            },
            {
                "step": 4,
                "title": "Write Content",
                "action": "Write each section following outline",
                "guidelines": [
                    "Use simple, clear language",
                    "Include specific examples",
                    "Add step-by-step instructions where applicable",
                    "Use short paragraphs (2-3 sentences)",
                ]
            },
            {
                "step": 5,
                "title": "Add Internal Links",
                "action": "Link to 2-3 related pages within new content",
                "example": "Mention 'Kontaktformular-Vorlage' and link to /templates/kontaktformular",
            },
            {
                "step": 6,
                "title": "Update TOC and Meta",
                "action": "If page has table of contents, add new sections. Update meta description to mention new topics.",
            }
        ],
        "target_increase": "20-30% more ranking keywords",
        "timeframe": "4-6 weeks to see impact",
    },
    
    "content_consolidation_v1": {
        "name": "Content Consolidation (Cannibalization Fix)",
        "description": "Merge competing pages to eliminate keyword cannibalization",
        "effort": "high",
        "estimated_time": "4-8 hours",
        "owner": "seo_team (technical) + content_team (merging)",
        "steps": [
            {
                "step": 1,
                "title": "Identify Cannibalizing Pages",
                "action": "Tool has flagged pages competing for same keyword. Review all URLs in cannibalization_group.",
            },
            {
                "step": 2,
                "title": "Choose Primary Page",
                "action": "Which page should be the main one?",
                "criteria": [
                    "Higher current ranking",
                    "More comprehensive content",
                    "Better matches search intent",
                    "Higher existing traffic",
                ]
            },
            {
                "step": 3,
                "title": "Merge Content",
                "action": "Copy unique sections from secondary pages to primary page",
                "checklist": [
                    "Preserve any unique information",
                    "Maintain best formatting from each",
                    "Keep all relevant examples",
                    "Combine FAQs if both have them",
                ]
            },
            {
                "step": 4,
                "title": "301 Redirect Setup",
                "action": "SEO team: Set up 301 redirects from secondary URLs to primary URL",
                "note": "This requires technical implementation - coordinate with SEO team",
            },
            {
                "step": 5,
                "title": "Update Internal Links",
                "action": "Find pages linking to old URLs, update to new URL",
            },
            {
                "step": 6,
                "title": "Monitor Rankings",
                "action": "Track keyword rank for 4-6 weeks post-consolidation",
                "expected": "Should rank higher with consolidated authority",
            }
        ],
        "warning": "Always coordinate with SEO team before deleting or redirecting pages",
        "impact": "15-25% rank improvement for target keyword",
    },
    
    "internal_linking_v1": {
        "name": "Strategic Internal Linking",
        "description": "Add internal links to distribute authority and help users navigate",
        "effort": "low",
        "estimated_time": "1-2 hours",
        "owner": "content_team",
        "steps": [
            {
                "step": 1,
                "title": "Identify Source Pages",
                "action": "Find 3-5 high-authority pages on your site to link FROM",
                "candidates": [
                    "Homepage",
                    "Popular blog posts",
                    "Feature pages with traffic",
                    "Related topic pages",
                ]
            },
            {
                "step": 2,
                "title": "Choose Anchor Text",
                "action": "Use keyword-rich but natural anchor text",
                "examples": {
                    "good": ["Formular-Erstellen-Tool", "kostenlose Formularvorlagen", "Online-Anmeldeformular erstellen"],
                    "bad": ["hier klicken", "diese Seite", "mehr erfahren"],
                }
            },
            {
                "step": 3,
                "title": "Add Links Contextually",
                "action": "Insert links within relevant paragraphs, not at bottom",
                "placement": "Natural mention within body content",
            },
            {
                "step": 4,
                "title": "Add Reciprocal Links",
                "action": "From target page, link back to source pages where relevant",
            }
        ],
        "impact": "10-15% rank boost from improved internal linking",
    },
}


def get_playbook(playbook_id: str) -> dict:
    """
    Retrieve a specific playbook by ID.
    
    Args:
        playbook_id: Playbook identifier (e.g., "ctr_optimization_v1")
    
    Returns:
        Dict with playbook details, or None if not found
    """
    return PLAYBOOKS.get(playbook_id)


def list_playbooks() -> list:
    """
    List all available playbooks.
    
    Returns:
        List of dicts with playbook summaries
    """
    summaries = []
    for playbook_id, playbook in PLAYBOOKS.items():
        summaries.append({
            "id": playbook_id,
            "name": playbook["name"],
            "description": playbook["description"],
            "effort": playbook["effort"],
            "owner": playbook["owner"],
        })
    return summaries


def export_playbooks_to_html(output_dir: str):
    """
    Export all playbooks to a single HTML reference document.
    """
    html = """
<!DOCTYPE html>
<html>
<head>
    <title>SEO Action Playbook Library</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 1000px; margin: 40px auto; padding: 20px; }
        h1 { color: #1e40af; border-bottom: 3px solid #3b82f6; padding-bottom: 10px; }
        .playbook { background: #f8fafc; padding: 30px; margin: 30px 0; border-radius: 12px; border-left: 5px solid #3b82f6; }
        .playbook h2 { color: #0f172a; margin-top: 0; }
        .meta { color: #64748b; margin: 10px 0; }
        .step { background: white; padding: 20px; margin: 15px 0; border-radius: 8px; }
        .step-title { font-weight: bold; color: #1e40af; margin-bottom: 10px; }
        .checklist { list-style: none; padding-left: 20px; }
        .checklist li:before { content: "☐ "; color: #10b981; font-weight: bold; }
        .example { background: #ecfdf5; padding: 15px; border-radius: 6px; margin: 10px 0; border-left: 3px solid #10b981; }
        .warning { background: #fef3c7; padding: 15px; border-radius: 6px; border-left: 3px solid #f59e0b; }
    </style>
</head>
<body>
    <h1>📚 SEO Action Playbook Library</h1>
    <p style="color: #64748b; font-size: 1.1em;">Step-by-step guides for content team to execute SEO improvements</p>
"""
    
    for playbook_id, playbook in PLAYBOOKS.items():
        html += f"""
    <div class="playbook" id="{playbook_id}">
        <h2>{playbook['name']}</h2>
        <p>{playbook['description']}</p>
        <div class="meta">
            <strong>Effort:</strong> {playbook['effort']} ({playbook.get('estimated_time', 'varies')})<br>
            <strong>Owner:</strong> {playbook['owner']}
        </div>
        
        <h3>Steps:</h3>
"""
        
        for step in playbook['steps']:
            html += f"""
        <div class="step">
            <div class="step-title">Step {step['step']}: {step['title']}</div>
            <p>{step['action']}</p>
"""
            
            if 'checklist' in step:
                html += '<ul class="checklist">'
                for item in step['checklist']:
                    html += f'<li>{item}</li>'
                html += '</ul>'
            
            if 'example' in step:
                html += f'<div class="example"><strong>Example:</strong> {step["example"]}</div>'
            
            html += '</div>'
        
        if 'common_mistakes' in playbook:
            html += '<h3>Common Mistakes to Avoid:</h3><ul>'
            for mistake in playbook['common_mistakes']:
                html += f'<li>{mistake}</li>'
            html += '</ul>'
        
        if 'warning' in playbook:
            html += f'<div class="warning"><strong>⚠️ Warning:</strong> {playbook["warning"]}</div>'
        
        html += '</div>'
    
    html += '</body></html>'
    
    output_path = os.path.join(output_dir, "seo_playbook_library.html")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ Playbook library exported → {output_path}")
    return output_path


if __name__ == "__main__":
    from config import OUTPUT_DIR
    export_playbooks_to_html(OUTPUT_DIR)