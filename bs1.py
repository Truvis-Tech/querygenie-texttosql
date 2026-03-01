            # Ensure JSONB fields are serialized and array fields are correctly typed
            sample_usage_data = json.dumps(metadata.get('sample_usage', []))
            # Coerce sample_values to a list of strings for TEXT[]
            raw_sample_values = metadata.get('sample_values', [])
            if isinstance(raw_sample_values, list):
                sample_values_clean = [v if isinstance(v, str) else str(v) for v in raw_sample_values]
            else:
                sample_values_clean = []
            # Coerce related_business_terms to a list of strings for TEXT[]
            raw_related_terms = metadata.get('related_business_terms', [])
            if isinstance(raw_related_terms, list):
                if any(isinstance(x, dict) for x in raw_related_terms):
                    related_terms_clean = []
                    for x in raw_related_terms:
                        if isinstance(x, dict):
                            # Prefer 'term' field if present; otherwise JSON-stringify the dict
                            related_terms_clean.append(x.get('term') if 'term' in x else json.dumps(x))
                        else:
                            related_terms_clean.append(x if isinstance(x, str) else str(x))
                else:
                    related_terms_clean = [x if isinstance(x, str) else str(x) for x in raw_related_terms]
            else:
                related_terms_clean = []
