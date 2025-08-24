console.log('Test started');

try {
  console.log('About to import express');
  const express = await import('express');
  console.log('Express imported successfully:', typeof express);
} catch (error) {
  console.error('Error importing express:', error);
}

console.log('Test completed');