defmodule CriteriaGenerator do
  @doc """
  used for webhooks
  """
  use Timex
  import Ecto.Query

  def perform(%{
    workspace_id: workspace_id,
    properties: properties,
    contact_id: contact_id
  }) do
    properties = Enum.reduce properties, [], fn property, acc ->
      if property.filter_type in ["event", "intent"] do
        acc
      else
        [property | acc]
      end
    end

    # generate the internal AND queries for each criteria
    criteria_query = generate_dynamic_query(%{query: false, properties: properties, workspace_id: workspace_id, contact_id: contact_id})

    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ], ^criteria_query)
  end

  # perform an update since an ID is being passed
  def perform(%{
    id: id,
    workspace_id: workspace_id,
    criteria: criteria,
    contact: contact
  }) do
    dynamic = Enum.reduce(criteria, false, fn item, query -> 
      # generate the internal AND queries for each criteria
      criteria_query = generate_dynamic_query(%{query: query, criteria: item, workspace_id: workspace_id, contact: contact})

      # Concatenate the queries together with an OR
      if query == false do
        dynamic([
          contact,
          contact_membership,
          contact_engagement,
          score,
        ], ^criteria_query)
      else
        dynamic([
          contact,
          contact_membership,
          contact_engagement,
          score,
        ], ^criteria_query or ^query)
      end
    end)

    dynamic
  end

  # perform an update since an ID is being passed
  def perform(%{
    workspace_id: workspace_id,
    criteria: criteria
  }) do
    dynamic = Enum.reduce(criteria, false, fn item, query -> 
      # createa params to save criteria to database

      # generate the internal AND queries for each criteria
      criteria_query = generate_dynamic_query(%{query: query, criteria: item, workspace_id: workspace_id})

      # Concatenate the queries together with an OR
      if query == false do
        dynamic([
          contact,
          contact_membership,
          contact_engagement,
          score,
        ], ^criteria_query)
      else
        dynamic([
          contact,
          contact_membership,
          contact_engagement,
          score,
        ], ^criteria_query or ^query)
      end
    end)

    dynamic
  end

  defp generate_dynamic_query(%{
    query: query,
    properties: properties,
    workspace_id: workspace_id,
    contact_id: contact_id
  }) do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      contact.workspace_id == ^workspace_id and contact.id == ^contact_id
    )

    Enum.reduce(properties, dynamic, fn property, query -> 
      # createa params to save criteria to database
      sanitized_property = Map.put(
        # because ENUM have to be upcase, let's first downcase it and then convert to an atom
        Map.put(property, :filter_type, String.to_atom(String.downcase(property.filter_type))),
        :field_type,
        String.to_atom(String.downcase(property.field_type))
      )
      property_query = format_property(sanitized_property, query)

      IO.inspect [FORMATTED_PROPERTY: property_query]

      # Concatenate the queries together with an OR
      dynamic([
        contact,
        contact_membership,
        contact_engagement,
        score,
      ], ^property_query and ^query)
    end)
  end

  defp generate_dynamic_query(%{
    query: query,
    criteria: criteria,
    workspace_id: workspace_id,
    contact: contact
  }) do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      contact.workspace_id == ^workspace_id and contact.id == ^contact.id
    )

    dynamic = Enum.reduce(criteria.properties, dynamic, fn property, query -> 
      # createa params to save criteria to database
      sanitized_property = Map.put(
        # because ENUM have to be upcase, let's first downcase it and then convert to an atom
        Map.put(property, :filter_type, String.to_atom(String.downcase(property.filter_type))),
        :field_type,
        String.to_atom(String.downcase(property.field_type))
      )
      property_query = format_property(sanitized_property, query)

      IO.inspect [FORMATTED_PROPERTY: property_query]

      # Concatenate the queries together with an OR
      dynamic([
        contact,
        contact_membership,
        contact_engagement,
        score,
      ], ^property_query and ^query)
    end)

    IO.inspect [DYNAMIC_QUERY: dynamic]

    dynamic
  end

  defp generate_dynamic_query(%{
    query: query,
    criteria: criteria,
    workspace_id: workspace_id
  }) do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      contact.workspace_id == ^workspace_id
    )

    dynamic = Enum.reduce(criteria.properties, dynamic, fn property, query -> 
      # createa params to save criteria to database
      sanitized_property = Map.put(
        # because ENUM have to be upcase, let's first downcase it and then convert to an atom
        Map.put(property, :filter_type, String.to_atom(String.downcase(property.filter_type))),
        :field_type,
        String.to_atom(String.downcase(property.field_type))
      )
      property_query = format_property(sanitized_property, query)

      IO.inspect [FORMATTED_PROPERTY: property_query]

      # Concatenate the queries together with an OR
      dynamic([
        contact,
        contact_membership,
        contact_engagement,
        score,
      ], ^property_query and ^query)
    end)

    IO.inspect [DYNAMIC_QUERY: dynamic]

    dynamic
  end

  # handle text field Type
  defp format_property(%{field_type: field_type} = property, query) when field_type in [:text, :multi, :select] do
    # convert the incoming property, semi-colon separated into
    # an array that can be compared against
    value = property.value
    
    generate_property(%{
      property: property,
      value: value
    })
  end

  # handle single checkbox field Type
  defp format_property(%{field_type: field_type} = property, query) when field_type == :boolean do
    # Convert the string "true"/"false" to a true boolean
    value = case property.value do
      "true" ->
        true
      "false" ->
        false
    end
    
    generate_property(%{
      property: property,
      value: value
    })
  end

  # handle date field Type
  defp format_property(%{field_type: field_type} = property, query) when field_type == :date do
    # convert the incoming property, semi-colon separated into
    # an array that can be compared against
    relative_options = ["more_than_ago", "more_than_from", "less_than_ago", "less_than_from"]

    value = cond do
      property.comparison in relative_options ->
        {integer, _} = Integer.parse(property.value)
        integer
      true ->
        Timex.parse!(property.value, "%B %e, %Y %Z", :strftime)
    end
    
    generate_property(%{
      property: property,
      value: value
    })
  end

  # handle number field Type
  defp format_property(%{field_type: field_type} = property, query) when field_type == :number do
    # convert the incoming property, to a real number
    {value, _} = Integer.parse(property.value)
    
    generate_property(%{
      property: property,
      value: value
    })
  end

  # handle currency field Type
  defp format_property(%{field_type: field_type} = property, query) when field_type == :currency do
    # convert the incoming property, to a real number
    {value, _} = Integer.parse(property.value)
    
    generate_property(%{
      property: property,
      value: value * 100
    })
  end

  # equal, not_equal, contain, not_contain, known, unknown, before, after, before_property, after_property, greater_than
  # greater_than_equal, less_than, less_than_equal, more_than_ago, more_than_from, less_than_ago, less_than_from

  # equals contact field of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "is_member" and
  filter_type == :list_membership do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      contact_membership.contact_list_id == ^field
  )
  end

  # equals contact field of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "is_not_member" and
  filter_type == :list_membership do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      contact_membership.contact_list_id != ^field
  )
  end

  # equals property property of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :property and
  field_type == :text do
    values = String.split(value, ";")
    values = build_equal_filter(values)

    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND obj->>'value' ILIKE ANY (?))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # equals property property of type checkbox
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :property and
  field_type == :boolean do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::boolean = ?)",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # equals property property of type multi
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :property and
  field_type == :multi do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (string_to_array(obj->>'value', ';') && string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # equals property property of type select
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :property and
  field_type == :select do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (string_to_array(obj->>'value', ';') && string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # equals property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) = date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # equals property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int = ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # not equals property property of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :text do
    values = String.split(value, ";")
    values = build_equal_filter(values)

    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND obj->>'value' NOT ILIKE ANY (?))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # not equals property property of type checkbox
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :boolean do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::boolean != ?)",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # not equals property property of type multi
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :multi do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND NOT (string_to_array(obj->>'value', ';') && string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # not equals property property of type select
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :select do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND NOT (string_to_array(obj->>'value', ';') && string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # not equals property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) != date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # not equals property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int != ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # contain property property of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "contain" and
  filter_type == :property and
  field_type == :text do
    values = String.split(value, ";")
    values = build_contain_filter(values)

    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND obj->>'value' ~* ?",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # contain property property of type multi
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "contain" and
  filter_type == :property and
  field_type == :multi do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (string_to_array(obj->>'value', ';') @> string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # contain property property of type select
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "contain" and
  filter_type == :property and
  field_type == :select do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (string_to_array(obj->>'value', ';') @> string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # doesn't contain property property of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_contain" and
  filter_type == :property and
  field_type == :text do
    values = String.split(value, ";")
    values = build_contain_filter(values)

    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND obj->>'value' !~* ?",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # doesn't contain property property of type multi
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_contain" and
  filter_type == :property and
  field_type == :multi do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND NOT (string_to_array(obj->>'value', ';') @> string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # doesn't contain property property of type select
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_contain" and
  filter_type == :property and
  field_type == :select do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND NOT (string_to_array(obj->>'value', ';') @> string_to_array(?, ';')))",
        field(contact, :properties),
        ^field,
        ^value
      )
    )
  end

  # is known property property of type multi
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :property and
  field_type in [:text, :boolean, :multi, :select, :date, :number] do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE (obj->>'property' = ?) IS NOT NULL)",
        field(contact, :properties),
        ^field
      )
    )
  end

  # is unknown property property of type multi
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :property and
  field_type in [:text, :boolean, :multi, :select, :date, :number] do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE (obj->>'property' = ?) IS NULL)",
        field(contact, :properties),
        ^field
      )
    )
  end

  # less than or equal to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_equal" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int <= ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # less than to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int < ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # more than or equal to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_equal" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int >= ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # more than to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int > ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # before property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "before" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) < date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # after property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "after" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) > date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # more than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_ago" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) < date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        ago(^value, "day")
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_from" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) > date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        from_now(^value, "day")
      )
    )
  end

  # less than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_ago" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) > date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        ago(^value, "day")
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_from" and
  filter_type == :property and
  field_type == :date do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND date_trunc('day', (obj->>'value')::timestamp) < date_trunc('day', ?::timestamp))",
        contact.properties,
        ^field,
        from_now(^value, "day")
      )
    )
  end

  # unknown engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "contact.last_reached IS NULL"
      )
    )
  end

  # known engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "contact.last_reached IS NOT NULL"
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) = date_trunc('day', ?::timestamp)",
        ^value
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) != date_trunc('day', ?::timestamp)",
        ^value
      )
    )
  end

  # before engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "before" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) < date_trunc('day', ?::timestamp)",
        ^value
      )
    )
  end

  # after property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "after" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) > date_trunc('day', ?::timestamp)",
        ^value
      )
    )
  end

  # more than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) < date_trunc('day', ?::timestamp)",
        ago(^value, "day")
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) > date_trunc('day', ?::timestamp)",
        from_now(^value, "day")
      )
    )
  end

  # less than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) > date_trunc('day', ?::timestamp)",
        ago(^value, "day")
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_reached" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact.last_reached) < date_trunc('day', ?::timestamp)",
        from_now(^value, "day")
      )
    )
  end

  # unknown engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "contact_engagement.inserted_at IS NULL AND contact_engagement.type = ?",
        :response
      )
    )
  end

  # known engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "contact_engagement.inserted_at IS NOT NULL AND contact_engagement.type = ?",
        :response
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) = date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :response
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) != date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :response
      )
    )
  end

  # before engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "before" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :response
      )
    )
  end

  # after property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "after" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :response
      )
    )
  end

  # more than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ago(^value, "day"),
        :response
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        from_now(^value, "day"),
        :response
      )
    )
  end

  # less than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ago(^value, "day"),
        :response
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_response" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        from_now(^value, "day"),
        :response
      )
    )
  end

  # unknown engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "contact_engagement.inserted_at IS NULL AND contact_engagement.type = ?",
        :click
      )
    )
  end

  # known engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "contact_engagement.inserted_at IS NOT NULL AND contact_engagement.type = ?",
        :click
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) = date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :click
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) != date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :click
      )
    )
  end

  # before engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "before" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :click
      )
    )
  end

  # after property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "after" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :click
      )
    )
  end

  # more than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ago(^value, "day"),
        :click
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        from_now(^value, "day"),
        :click
      )
    )
  end

  # less than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ago(^value, "day"),
        :click
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_click" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        from_now(^value, "day"),
        :click
      )
    )
  end

  # unknown engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "contact_engagement.inserted_at IS NULL AND contact_engagement.type = ?",
        :purchase
      )
    )
  end

  # known engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "contact_engagement.inserted_at IS NOT NULL AND contact_engagement.type = ?",
        :purchase
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) = date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :purchase
      )
    )
  end

  # equal engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) != date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :purchase
      )
    )
  end

  # before engagement property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "before" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :purchase
      )
    )
  end

  # after property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "after" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ^value,
        :purchase
      )
    )
  end

  # more than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ago(^value, "day"),
        :purchase
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        from_now(^value, "day"),
        :purchase
      )
    )
  end

  # less than N days ago property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_ago" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) > date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        ago(^value, "day"),
        :purchase
      )
    )
  end

  # more than N days from property property of type date
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_from" and
  filter_type == :engagement and
  field_type == :date and
  field == "last_purchase" do
    dynamic = dynamic([
      contact,
      contact_engagement,
      score,
    ],
      fragment(
        "date_trunc('day', contact_engagement.inserted_at) < date_trunc('day', ?::timestamp) AND contact_engagement.type = ?",
        from_now(^value, "day"),
        :purchase
      )
    )
  end

  # not equals property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :property and
  field_type == :number do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "exists (SELECT * FROM jsonb_array_elements(?) obj WHERE obj->>'property' = ? AND (obj->>'value')::int != ?)",
        contact.properties,
        ^field,
        ^value
      )
    )
  end

  # equals survey_data property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score = ? AND score.type = ?",
        ^value,
        :csat
      )
    )
  end

  # not equals survey_data property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score != ? AND score.type = ?",
        ^value,
        :csat
      )
    )
  end

  # less than or equal to survey_data property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_equal" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score <= ? AND score.type = ?",
        ^value,
        :csat
      )
    )
  end

  # less than to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score < ? AND score.type = ?",
        ^value,
        :csat
      )
    )
  end

  # more than or equal to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_equal" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score >= ? AND score.type = ?",
        ^value,
        :csat
      )
    )
  end

  # more than to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score > ? AND score.type = ?",
        ^value,
        :csat
      )
    )
  end

  # is unknown survey_data field of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.contact_id IS NULL AND score.type = ?",
        :csat
      )
    )
  end

  # is known survey_data field of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :survey_data and
  field_type == :number and
  field == "csat" do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.contact_id IS NOT NULL AND score.type = ?",
        :csat
      )
    )
  end

  # equals survey_data property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "equal" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score = ? AND score.type = ?",
        ^value,
        :nps
      )
    )
  end

  # not equals survey_data property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "not_equal" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score != ? AND score.type = ?",
        ^value,
        :nps
      )
    )
  end

  # less than or equal to survey_data property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than_equal" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score <= ? AND score.type = ?",
        ^value,
        :nps
      )
    )
  end

  # less than to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "less_than" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score < ? AND score.type = ?",
        ^value,
        :nps
      )
    )
  end

  # more than or equal to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than_equal" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score >= ? AND score.type = ?",
        ^value,
        :nps
      )
    )
  end

  # more than to property property of type number
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "more_than" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic = dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.score > ? AND score.type = ?",
        ^value,
        :nps
      )
    )
  end

  # is unknown survey_data field of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "unknown" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.contact_id IS NULL AND score.type = ?",
        :nps
      )
    )
  end

  # is known survey_data field of type text
  defp generate_property(%{
    property: %{
      comparison: comparison,
      filter_type: filter_type,
      field_type: field_type,
      field: field
    } = property,
    value: value
  }) when comparison == "known" and
  filter_type == :survey_data and
  field_type == :number and
  field != "csat" do
    dynamic([
      contact,
      contact_membership,
      contact_engagement,
      score,
    ],
      fragment(
        "score.contact_id IS NOT NULL AND score.type = ?",
        :nps
      )
    )
  end

  defp build_equal_filter(filters) do
    all_filters = Enum.join(filters, ",")
    "{" <> all_filters <> "}"
  end

  defp build_contain_filter(filters) do
    all_filters = Enum.join(filters, "|")
    "(" <> all_filters <> ")"
  end
end
