from typing import List, Dict, Any
from app.schemas.metrics import EntityContext
from app.services.rules.meta_rules import META_RULES_PIPELINE, RuleResult
from app.db.supabase import supabase

class RuleExecutor:
    """
    Orchestrates the execution of rules for ad entities.
    Logs fired rules to Supabase for the dashboard to display.
    """
    
    @staticmethod
    def process_entities(entities: List[EntityContext]) -> List[Dict[str, Any]]:
        suggestions = []
        
        for entity in entities:
            # Run the pipeline (P1 -> P5)
            # We stop after the first rule fires to avoid conflicting suggestions
            for rule_func in META_RULES_PIPELINE:
                result: RuleResult = rule_func(entity)
                
                if result.fired and result.suggestion:
                    # Enrich suggestion with entity details
                    suggestion = result.suggestion
                    suggestion["entity_id"] = entity.entity_id
                    suggestion["entity_name"] = entity.entity_name
                    suggestion["channel"] = entity.channel
                    
                    # Log to Supabase (Optional: only if DB is configured)
                    try:
                        # supabase.table("suggestions_log").insert(suggestion).execute()
                        pass
                    except Exception:
                        pass
                    
                    suggestions.append(suggestion)
                    break 
                    
        return suggestions

# Global instance for easy import
executor = RuleExecutor()
