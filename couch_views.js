{
  "language":"javascript",
  "views":
  {
    "index":
    {
      "map":"function(doc)
      { 
        if(doc.Type == 'offer'){
          emit([doc.Name,doc.Date],
            {
            // pies
            'Commission':doc.Commission,
            'Chargeback':doc.Commission - doc.PercentPerSale / 10.0,

            // Sparklines
            'Gravity':doc.Gravity, 
            'Referred':doc.Referred, 
            'Rebill':doc.TotalRebillAmt,
    
            // TODO: fetch categories/popularity
            // TODO: fetch keyword classification counts
            }
          )
        }
      }","reduce":"function(keys,values){return values}"
    }
  }
}
