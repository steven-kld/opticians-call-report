function doPost(e) {
    try {
      const requestData = JSON.parse(e.postData.contents);
      
      if (!requestData.report) {
        throw new Error("Missing 'report' key in the request.");
      }
      
      if (!requestData.filename) {
        throw new Error("Missing 'filename' key in the request.");
      }
      
  
      const reportData = requestData.report;
      const filename = requestData.filename;
      Logger.log(reportData);
  
      var docUrl = createSpreadsheet(filename, reportData);
      var response = {
        doc_url: docUrl
      };
      
      return ContentService.createTextOutput(JSON.stringify(response))
        .setMimeType(ContentService.MimeType.JSON);
  
    } catch (err) {
      Logger.log('Error in doPost: ' + err.message);
      var errorResponse = {
        error: 'Failed to create document: ' + err.message,
        doc_url: null
      };
      return ContentService.createTextOutput(JSON.stringify(errorResponse))
        .setMimeType(ContentService.MimeType.JSON);
    }
  }
  
  function test() {
    var data = [{'': 'Total Calls', 'Cheadle': 22, 'Heald Green': 49, 'Heckmondwike': 70, 'Middleton': 81, 'Winsford': 94, 'Total': 316}, {'': 'Duration (total)', 'Cheadle': '19:53', 'Heald Green': '104:33', 'Heckmondwike': '43:38', 'Middleton': '57:35', 'Winsford': '139:40', 'Total': '365:19'}, {'': '', 'Cheadle': '', 'Heald Green': '', 'Heckmondwike': '', 'Middleton': '', 'Winsford': '', 'Total': ''}, {'': 'Inbound Calls', 'Cheadle': 13, 'Heald Green': 17, 'Heckmondwike': 18, 'Middleton': 28, 'Winsford': 40, 'Total': 116}, {'': 'Duration (inbound)', 'Cheadle': '11:59', 'Heald Green': '38:21', 'Heckmondwike': '16:09', 'Middleton': '20:31', 'Winsford': '35:41', 'Total': '122:41'}, {'': 'Redirected (inbound)', 'Cheadle': 13, 'Heald Green': 17, 'Heckmondwike': 18, 'Middleton': 28, 'Winsford': 40, 'Total': 116}, {'': 'Answered Directly (inbound)', 'Cheadle': 7, 'Heald Green': 7, 'Heckmondwike': 11, 'Middleton': 15, 'Winsford': 26, 'Total': 66}, {'': 'Voicemails Received (inbound)', 'Cheadle': 2, 'Heald Green': 10, 'Heckmondwike': 1, 'Middleton': 9, 'Winsford': 8, 'Total': 30}, {'': 'Dropped/Unanswered (inbound)', 'Cheadle': 4, 'Heald Green': 0, 'Heckmondwike': 6, 'Middleton': 4, 'Winsford': 6, 'Total': 20}, {'': 'Dropped & Voicemail Recalled (inbound)', 'Cheadle': 4, 'Heald Green': 8, 'Heckmondwike': 3, 'Middleton': 10, 'Winsford': 3, 'Total': 28}, {'': '% of Calls Recalled (inbound)', 'Cheadle': '67%', 'Heald Green': '80%', 'Heckmondwike': '43%', 'Middleton': '77%', 'Winsford': '21%', 'Total': '56%'}, {'': 'Booked (from inbound recorded)', 'Cheadle': 0, 'Heald Green': 0, 'Heckmondwike': 2, 'Middleton': 1, 'Winsford': 0, 'Total': 3}, {'': '', 'Cheadle': '', 'Heald Green': '', 'Heckmondwike': '', 'Middleton': '', 'Winsford': '', 'Total': ''}, {'': 'Outbound Calls', 'Cheadle': 6, 'Heald Green': 32, 'Heckmondwike': 52, 'Middleton': 53, 'Winsford': 50, 'Total': 193}, {'': 'Duration (outbound)', 'Cheadle': '7:45', 'Heald Green': '66:12', 'Heckmondwike': '27:29', 'Middleton': '37:04', 'Winsford': '95:30', 'Total': '234:00'}, {'': 'Dropped/Unanswered (outbound)', 'Cheadle': 2, 'Heald Green': 10, 'Heckmondwike': 19, 'Middleton': 14, 'Winsford': 13, 'Total': 58}, {'': '', 'Cheadle': '', 'Heald Green': '', 'Heckmondwike': '', 'Middleton': '', 'Winsford': '', 'Total': ''}, {'': 'Outbound Recorded Calls', 'Cheadle': 4, 'Heald Green': 6, 'Heckmondwike': 38, 'Middleton': 10, 'Winsford': 0, 'Total': 58}, {'': 'Answered Calls (outbound recorded)', 'Cheadle': 2, 'Heald Green': 4, 'Heckmondwike': 20, 'Middleton': 7, 'Winsford': 0, 'Total': 33}, {'': 'Voicemail (outbound recorded)', 'Cheadle': 2, 'Heald Green': 1, 'Heckmondwike': 12, 'Middleton': 3, 'Winsford': 0, 'Total': 18}, {'': 'Dropped/Unanswered (outbound recorded)', 'Cheadle': 0, 'Heald Green': 1, 'Heckmondwike': 6, 'Middleton': 0, 'Winsford': 0, 'Total': 7}, {'': 'Proactive Recalls (outbound recorded)', 'Cheadle': 1, 'Heald Green': 1, 'Heckmondwike': 6, 'Middleton': 2, 'Winsford': 0, 'Total': 10}, {'': 'Booked from Proactive (outbound recorded)', 'Cheadle': 1, 'Heald Green': 0, 'Heckmondwike': 4, 'Middleton': 2, 'Winsford': 0, 'Total': 7}, {'': 'Conversion Rate Proactive % (outbound recorded)', 'Cheadle': '100%', 'Heald Green': '0%', 'Heckmondwike': '67%', 'Middleton': '100%', 'Winsford': '0%', 'Total': '70%'}, {'': 'New Patient Calls (outbound recorded)', 'Cheadle': 0, 'Heald Green': 0, 'Heckmondwike': 0, 'Middleton': 0, 'Winsford': 0, 'Total': 0}];
    console.log(createSpreadsheet("omg1488", data))
  }
  
  function createSpreadsheet(filename, reportData) {
    const ss = SpreadsheetApp.create(filename);
    const sheet = ss.getActiveSheet();
    
    let rowIndex = 1;
    let headers = [];
    let isFirstSection = true;
  
    reportData.forEach(row => {
      const isSpacer = Object.values(row).every(val => val === '' || val === null);
      if (isSpacer) {
        rowIndex++;
        return;
      }
      
      const hasTotal = row.hasOwnProperty('Total') && Object.keys(row).length > 1;
      const isSectionHeader = row[''] && row[''] !== '' && !hasTotal;
  
      if (isSectionHeader) {
        if (!isFirstSection) {
          rowIndex++;
        }
        sheet.getRange(rowIndex, 1).setValue(row['']).setFontWeight('bold');
        rowIndex++;
        isFirstSection = false;
        headers = [];
      } else if (hasTotal) {
        if (headers.length === 0) {
          headers = Object.keys(row);
          const headerRow = headers.map(key => key === '' ? 'Metric' : key);
          const headerRange = sheet.getRange(rowIndex, 1, 1, headerRow.length);
          headerRange.setValues([headerRow]).setFontWeight('bold');
          headerRange.setHorizontalAlignment('left');
          headerRange.setBorder(true, true, true, true, true, true, '#000000', SpreadsheetApp.BorderStyle.SOLID);
          rowIndex++;
        }
        
        const rowValues = headers.map(key => {
          const value = row[key];
          return (value === null || value === undefined) ? '' : value;
        });
        
        const dataRange = sheet.getRange(rowIndex, 1, 1, rowValues.length);
        dataRange.setValues([rowValues]);
        dataRange.setHorizontalAlignment('left');
        dataRange.setBorder(true, true, true, true, true, true, '#000000', SpreadsheetApp.BorderStyle.SOLID);
        
        rowIndex++;
      }
    });
  
    // Autofit all columns for a clean look
    sheet.autoResizeColumns(1, sheet.getLastColumn());
    
    // New code to set custom column widths
    const standardWidth = 100;
    const lastColumn = sheet.getLastColumn();
    
    // Set width of Column A to 3x the standard width
    sheet.setColumnWidth(1, standardWidth * 3);
    
    // Set standard width for all other columns
    for (let i = 2; i <= lastColumn; i++) {
      sheet.setColumnWidth(i, standardWidth);
    }
  
    // Get the file and set sharing permissions
    const fileId = ss.getId();
    const file = DriveApp.getFileById(fileId);
    file.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.EDIT);
  
    return ss.getUrl();
  }