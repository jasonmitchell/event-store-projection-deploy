fromAll()
  .whenAny(function(s, e) {
    if (e.eventType[0] !== '$')
      linkTo("non_system", e);
  })
  .outputState();